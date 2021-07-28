import json
import logging
import os
from collections import OrderedDict, defaultdict

import orjson

from firepit import raft
from firepit.exceptions import DuplicateTable


logger = logging.getLogger(__name__)


def _strip_prefix(s, prefix):
    return s[len(prefix):] if s.startswith(prefix) else s


class JsonWriter:
    """
    Writes STIX objects to JSON files, one file per type
    """

    def __init__(self, filedir):
        self.filenames = {}
        self.files = {}
        self.filedir = filedir
        self.props = {}

    def _get_fp(self, obj_type):
        if obj_type not in self.files:
            filename = os.path.join(self.filedir, obj_type) + '.json'
            self.files[obj_type] = open(filename, 'w')
            self.filenames[obj_type] = filename
        return self.files[obj_type]

    def new_type(self, obj_type, schema):
        self.props[obj_type] = schema

    def new_property(self, obj_type, prop_name, prop_type):
        self.props[obj_type][prop_name] = prop_type

    def write_records(self, obj_type, records, _, replace, query_id):
        if replace:
            raise Exception('"replace" not supported when writing JSON')
        fp = self._get_fp(obj_type)
        buf = '\n'.join([str(orjson.dumps(rec), 'utf-8') for rec in records])
        fp.write('{}\n'.format(buf))

    def types(self):
        return list(self.files.keys())

    def properties(self, obj_type):
        return self.props.get(obj_type, {})

    def __del__(self):
        for fp in self.files.values():
            if fp:
                fp.close()


class SqlWriter:
    """
    Writes STIX objects to a SQL DB, one table per type
    """

    def __init__(self, filedir, store, prefix='',
                 placeholder='?',
                 infer_type=None):
        self.filedir = filedir
        self.store = store
        if prefix and not prefix.endswith('_'):
            self.prefix = prefix + '_'
        else:
            self.prefix = prefix
        self.placeholder = placeholder
        self.infer_type = infer_type
        self.schemas = defaultdict(OrderedDict)

    def _execute(self, stmt, cursor=None):
        return self.store._execute(stmt, cursor)

    def _insert(self, cursor, tablename, obj):
        # We will see "duplicate" identity objects (e.g. same identity in multiple bundles)
        colnames = obj.keys()
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([self.placeholder] * len(obj))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;'
        values = tuple([str(orjson.dumps(value), 'utf-8')
                        if isinstance(value, list) else value for value in obj.values()])
        #logger.debug('_insert: "%s"', stmt)
        cursor.execute(stmt, values)

    def _replace(self, cursor, tablename, obj):
        colnames = obj.keys()
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([self.placeholder] * len(obj))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders}) ON CONFLICT (id) DO '
        valnames = [f'"{col}" = EXCLUDED."{col}"' for col in colnames if col != 'id']
        valnames = ', '.join(valnames)
        stmt += f'UPDATE SET {valnames};'
        tmp = [str(orjson.dumps(value), 'utf-8')
               if isinstance(value, list) else value for value in obj.values()]
        values = tuple(tmp)
        logger.debug('_replace: "%s" values %s', stmt, values)
        cursor.execute(stmt, values)

    def new_type(self, obj_type, schema):
        tablename = f'{self.prefix}{obj_type}'
        self.store._create_table(tablename, schema)

    def new_property(self, obj_type, prop_name, prop_type):
        tablename = f'{self.prefix}{obj_type}'
        self.store._add_column(tablename, prop_name, prop_type)

    def write_records(self, obj_type, records, _, replace, query_id):
        tablename = f'{self.prefix}{obj_type}'
        try:
            cursor = self.store.connection.cursor()
            cursor.execute('BEGIN')
            if replace:
                for obj in records:
                    self._replace(cursor, tablename, obj)
            else:
                self.store.upsert_many(cursor, tablename, records, query_id)
            cursor.execute('COMMIT')
        finally:
            cursor.close()

    def types(self):
        tables = self.store.tables()
        return [_strip_prefix(table, self.prefix) for table in tables]

    def properties(self, obj_type):
        tablename = f'{self.prefix}{obj_type}'
        return self.store.schema(tablename)


class SplitWriter:
    """
    Writes STIX objects using `writer`.  This class will track schema
    changes and store `batchsize` objects in memory before passing to
    `writer`.

    """

    def __init__(self, writer, batchsize=10, extras=None, replace=False, query_id=None):
        self.schemas = {}
        self.writer = writer
        self.batchsize = batchsize
        self.records = defaultdict(list)
        self.extras = extras or {}
        self.replace = replace
        self.query_id = query_id
        self._load_schemas()

    def __del__(self):
        pass

    def _load_schema(self, obj_type):
        return {col['name']: col['type'] for col in self.writer.properties(obj_type)}

    def _load_schemas(self):
        for obj_type in self.writer.types():
            self.schemas[obj_type] = self._load_schema(obj_type)

    def write(self, obj):
        """Consume `obj` (actual writing to storage may be deferred)"""
        obj.update(self.extras)
        obj_type = obj['type']
        schema = self.schemas.get(obj_type)
        add_table = False
        add_col = False
        new_columns = {}
        if not schema:
            schema = {}
            add_table = True
        obj = {key: val for key, val in obj.items() if len(key) <= 63}
        for key, value in obj.items():
            if key not in schema:
                if not add_table:
                    add_col = True
                col_type = self.writer.infer_type(key, value)
                schema[key] = col_type
                new_columns[key] = col_type
        if add_table:
            self.schemas[obj_type] = schema
            try:
                self.writer.new_type(obj_type, schema)
            except DuplicateTable:
                # Refresh schemas
                loaded_schema = self._load_schema(obj_type)
                new_columns = {}
                for key in set(schema) - set(loaded_schema):
                    new_columns[key] = schema[key]
                    add_col = True
        if add_col:
            for col, col_type in new_columns.items():
                self.writer.new_property(obj_type, col, col_type)
        self.records[obj_type].append(obj)
        if len(self.records[obj_type]) % self.batchsize == 0:
            self.writer.write_records(obj_type, self.records[obj_type], schema, self.replace, self.query_id)
            self.records[obj_type] = []

    def close(self):
        if self.batchsize > 1:
            for obj_type, recs in self.records.items():
                if recs:
                    # We've already added any necessary tables or columns
                    self.writer.write_records(obj_type, recs, self.schemas[obj_type], self.replace, self.query_id)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', level=logging.DEBUG)
    import argparse
    parser = argparse.ArgumentParser('Split STIX bundles by object type')
    parser.add_argument('-d', '--directory', metavar='DIR', default='.')
    parser.add_argument('-f', '--format', metavar='FMT', default='json')
    parser.add_argument('-b', '--batchsize', metavar='N', default=100, type=int)
    parser.add_argument('-p', '--prefix', metavar='PREFIX', default='test_')
    parser.add_argument('-n', '--dbname', metavar='DBNAME', default='test.db')
    parser.add_argument('ops', metavar='OP,...')
    parser.add_argument('filename', metavar='FILENAME', nargs='+')
    args = parser.parse_args()

    if args.format == 'json':
        writer = JsonWriter(args.directory)
    elif args.format in ['sql', 'sqlite', 'sqlite3']:
        import sqlite3
        conn = sqlite3.connect(args.dbname)
        writer = SqlWriter(args.directory, conn, prefix=args.prefix)
    else:
        raise NotImplementedError(args.format)

    splitter = SplitWriter(writer, batchsize=args.batchsize)
    from pyinstrument import Profiler
    profiler = Profiler()
    profiler.start()
    for f in args.filename:
        for result in raft.transform(args.ops.split(','), f):
            splitter.write(result)
    profiler.stop()
    print(profiler.output_text(unicode=True, color=True))
    splitter.close()
