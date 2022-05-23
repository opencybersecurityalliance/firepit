import logging
import os
from collections import OrderedDict, defaultdict

import ujson

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

    @staticmethod
    def infer_type(key, value):
        return 'N/A'

    def new_type(self, obj_type, schema):
        self.props[obj_type] = schema

    def new_property(self, obj_type, prop_name, prop_type):
        self.props[obj_type][prop_name] = prop_type

    def write_records(self, obj_type, records, schema, replace, query_id):
        if replace:
            raise Exception('"replace" not supported when writing JSON')
        fp = self._get_fp(obj_type)
        for record in records:
            obj = OrderedDict(zip(schema.keys(), record))
            buf = ujson.dumps(obj, ensure_ascii=False)
            fp.write(buf)

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
                 infer_type=None,
                 **kwargs):
        self.filedir = filedir
        self.store = store
        if prefix and not prefix.endswith('_'):
            self.prefix = prefix + '_'
        else:
            self.prefix = prefix
        self.placeholder = placeholder
        self.infer_type = infer_type
        self.schemas = defaultdict(OrderedDict)
        self.kwargs = kwargs

    def _execute(self, stmt, cursor=None):
        return self.store._execute(stmt, cursor)

    def _replace(self, cursor, tablename, obj, schema):
        colnames = schema.keys()
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([self.placeholder] * len(obj))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders}) ON CONFLICT (id) DO '
        valnames = [f'"{col}" = EXCLUDED."{col}"' for col in colnames if col != 'id']
        valnames = ', '.join(valnames)
        stmt += f'UPDATE SET {valnames};'
        tmp = [ujson.dumps(value, ensure_ascii=False)
               if isinstance(value, list) else value for value in obj]
        values = tuple(tmp)
        logger.debug('_replace: "%s" values %s', stmt, values)
        cursor.execute(stmt, values)

    def new_type(self, obj_type, schema):
        tablename = f'{self.prefix}{obj_type}'
        self.store._create_table(tablename, schema)

    def new_property(self, obj_type, prop_name, prop_type):
        tablename = f'{self.prefix}{obj_type}'
        self.store._add_column(tablename, prop_name, prop_type)

    def write_records(self, obj_type, records, schema, replace, query_id):
        tablename = f'{self.prefix}{obj_type}'
        try:
            self.store.connection.commit()
            cursor = self.store.connection.cursor()
            cursor.execute('BEGIN')
            if replace:
                for obj in records:
                    self._replace(cursor, tablename, obj, schema)
            else:
                kwargs = {k: v for k, v in self.kwargs.items() if k != 'query_id'}
                self.store.upsert_many(cursor, tablename, records, query_id, schema, **kwargs)
            cursor.execute('COMMIT')
        finally:
            cursor.close()

    def types(self, private):
        tables = self.store.types(private)
        return [_strip_prefix(table, self.prefix) for table in tables]

    def properties(self, obj_type):
        tablename = f'{self.prefix}{obj_type}'
        return self.store.schema(tablename)


class RecordList:
    def __init__(self, id_idx):
        self.id_idx = id_idx
        self.reset()

    def reset(self):
        self.records = {} if self.id_idx else []

    def append(self, record):
        if self.id_idx:
            rec_id = record[self.id_idx]
            if rec_id in self.records:
                # Update record instead
                rec = self.records[rec_id]
                for i, val in enumerate(record):
                    if val is not None:
                        rec[i] = val
            else:
                self.records[rec_id] = record
        else:
            self.records.append(record)

    def __iter__(self):
        if self.id_idx:
            yield from self.records.values()
        else:
            yield from self.records

    def __len__(self):
        return len(self.records)


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
        self.records = {}
        self.extras = extras or {}
        self.replace = replace
        self.query_id = query_id
        self._load_schemas()

    def __del__(self):
        pass

    def _load_schema(self, obj_type):
        return {col['name']: col['type'] for col in self.writer.properties(obj_type)}

    def _load_schemas(self):
        for obj_type in self.writer.types(True):
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
        new_obj = {}
        for key, value in obj.items():
            if len(key) > 63 or key in ['type', 'spec_version']:
                continue
            new_obj[key] = value
            if key not in schema:
                if not add_table:
                    add_col = True
                col_type = self.writer.infer_type(key, value)
                schema[key] = col_type
                new_columns[key] = col_type
        obj = new_obj
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
        if obj_type in self.records:
            reclist = self.records[obj_type]
        else:
            try:
                id_idx = list(schema.keys()).index('id')
            except ValueError:
                id_idx = None
            reclist = RecordList(id_idx)
            self.records[obj_type] = reclist
        if add_col:
            for col, col_type in new_columns.items():
                for rec in reclist:
                    rec.append(None)
                self.writer.new_property(obj_type, col, col_type)
        self.records[obj_type].append([obj.get(col) for col in schema.keys()])
        if len(self.records[obj_type]) % self.batchsize == 0:
            self.writer.write_records(obj_type, self.records[obj_type], schema, self.replace, self.query_id)
            self.records[obj_type].reset()

    def close(self):
        if self.batchsize > 1:
            for obj_type, recs in self.records.items():
                if recs:
                    # We've already added any necessary tables or columns
                    self.writer.write_records(obj_type, recs, self.schemas[obj_type], self.replace, self.query_id)
