import logging
import re
import uuid

import orjson

from firepit import raft
from firepit.exceptions import IncompatibleType
from firepit.exceptions import InvalidAttr
from firepit.exceptions import InvalidObject
from firepit.exceptions import StixPatternError
from firepit.props import auto_agg
from firepit.props import primary_prop
from firepit.splitter import SplitWriter
from firepit.stix20 import stix2sql
from firepit.validate import validate_name
from firepit.validate import validate_path

logger = logging.getLogger(__name__)

STIX_TRANSFORMS = [raft.preserve, raft.invert, raft.markroot,
                   raft.makeid, raft.nest, raft.promote, raft.normalize]


def _transform(ops, filename):
    for obj in raft.get_objects(filename, ['identity', 'observed-data']):
        inputs = [obj]
        for op in ops:
            results = []
            for i in inputs:
                results.extend(op(i))
            inputs = results
        for result in results:
            yield result


def infer_type(key, value):
    if key == 'id':
        rtype = 'TEXT UNIQUE'
    elif key == 'number_observed':
        rtype = 'NUMERIC'
    elif isinstance(value, int):
        rtype = 'NUMERIC'
    elif isinstance(value, float):
        rtype = 'REAL'
    elif isinstance(value, list):
        rtype = 'TEXT'
    else:
        rtype = 'TEXT'
    return rtype


class SqlStorage:
    def __init__(self):
        self.connection = None  # Python DB API connection object
        self.placeholder = '%s'  # Derived class can override this

        # Functions to use for min/max text.  It can vary - sqlite3
        # uses MIN/MAX, postgresql uses LEAST/GREATEST
        self.text_min = 'MIN'
        self.text_max = 'MAX'

        # Function that returns first non-null arg_type
        self.ifnull = 'IFNULL'

    def _get_writer(self, prefix):
        """Get a DB inserter object"""
        # This is DB-specific
        raise NotImplementedError('SqlStorage._get_writer')

    def _initdb(self, cursor):
        """Do some initial DB setup"""
        stmt = ('CREATE TABLE IF NOT EXISTS "__symtable" '
                '(name TEXT, type TEXT, appdata TEXT);')
        self._execute(stmt, cursor)
        stmt = ('CREATE TABLE IF NOT EXISTS "__membership" '
                '(sco_id TEXT, var TEXT);')
        self._execute(stmt, cursor)
        stmt = ('CREATE TABLE IF NOT EXISTS "__queries" '
                '(sco_id TEXT, query_id TEXT);')
        self._execute(stmt, cursor)
        self.connection.commit()
        cursor.close()

    def _new_name(self, cursor, name, sco_type):
        stmt = ('INSERT INTO "__symtable" (name, type)'
                f' VALUES ({self.placeholder}, {self.placeholder});')
        cursor.execute(stmt, (name, sco_type))

    def _drop_name(self, cursor, name):
        stmt = f'DELETE FROM "__symtable" WHERE name = {self.placeholder};'
        cursor.execute(stmt, (name,))

    def _execute(self, statement, cursor=None):
        """Private wrapper for logging SQL statements"""
        logger.debug('Executing statement: %s', statement)
        if not cursor:
            cursor = self.connection.cursor()
        cursor.execute(statement)
        return cursor

    def _command(self, cmd, cursor=None):
        """Private wrapper for logging SQL commands"""
        logger.debug('Executing command: %s', cmd)
        cursor = self.connection.cursor()
        cursor.execute(cmd)
        self.connection.commit()

    def _query(self, query, values=None):
        """Private wrapper for logging SQL query"""
        logger.debug('Executing query: %s', query)
        cursor = self.connection.cursor()
        if not values:
            values = ()
        cursor.execute(query, values)
        self.connection.commit()
        return cursor

    def _select(self, tvname, cols="*", sortby=None, groupby=None,
                ascending=True, limit=None, offset=None, where=None):
        """Generate a SELECT query on table or view `tvname`"""
        # TODO: Deprecate this in favor of query module
        validate_name(tvname)
        if cols != "*":
            cols = ", ".join([f'"{col}"' if not col.startswith("'") else col for col in cols])

        stmt = f'SELECT {cols} FROM "{tvname}"'
        if where:
            stmt += f' WHERE {where}'
        if groupby:
            validate_path(groupby)

            # For grouping, we need to aggregate data in the columns.
            aggs = [
                'MIN("type") as "type"',
                f'"{groupby}"',
            ]
            sco_type = self.table_type(tvname)
            for col in self.schema(tvname):
                # Don't aggregate the column we used for grouping
                if col['name'] == groupby:
                    continue
                agg = auto_agg(sco_type, col['name'], col['type'])
                if agg:
                    aggs.append(agg)
            group_cols = ', '.join(aggs)
            stmt = f'SELECT {group_cols} from "{tvname}"'
            stmt += f' GROUP BY "{groupby}"'
        if sortby:
            validate_path(sortby)
            stmt += f' ORDER BY "{sortby}" ' + ('ASC' if ascending else 'DESC')
        if limit:
            if not isinstance(limit, int):
                raise TypeError('LIMIT must be an integer')
            stmt += f' LIMIT {limit}'
        if offset:
            if not isinstance(offset, int):
                raise TypeError('LIMIT must be an integer')
            stmt += f' OFFSET {offset}'
        return stmt

    def _create_table(self, tablename, columns):
        stmt = f'CREATE TABLE "{tablename}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in columns.items()])
        stmt += ');'
        logger.debug('_create_table: "%s"', stmt)
        cursor = self._execute(stmt)
        if 'x_contained_by_ref' in columns:
            self._execute(f'CREATE INDEX "{tablename}_obs" ON "{tablename}" ("x_contained_by_ref");', cursor)
        self.connection.commit()
        cursor.close()

    def _add_column(self, tablename, prop_name, prop_type):
        stmt = f'ALTER TABLE "{tablename}" ADD COLUMN "{prop_name}" {prop_type};'
        logger.debug('new_property: "%s"', stmt)
        self._execute(stmt)

    def _create_view(self, viewname, select, sco_type, deps=None, cursor=None):
        # This is DB-specific
        raise NotImplementedError('Storage._create_view')

    def _get_view_def(self, viewname):
        # This is DB-specific
        raise NotImplementedError('Storage._get_view_def')

    def _extract(self, viewname, sco_type, tablename, pattern, query_id=None):
        """Extract rows from `tablename` to create view `viewname`"""
        validate_name(viewname)
        validate_name(tablename)
        try:
            where = stix2sql(pattern, sco_type) if pattern else None
        except Exception as e:
            logger.error('%s', e)
            raise StixPatternError(pattern) from e
        if query_id:
            clause = f"query_id = '{query_id}'"
            if where:
                where = f"{clause} AND ({where})"
            else:
                where = clause

        # Need to convert viewname from identifier to string, so use single quotes
        namestr = f"'{viewname}'"
        cursor = self._execute('BEGIN;')

        # If we're reassigning an existing viewname, we need to drop old membership
        old_type = None
        if viewname in self.views():
            old_type = self.table_type(viewname)
            stmt = f'DELETE FROM __membership WHERE var = {namestr};'
            cursor = self._execute(stmt, cursor)

        if tablename in self.tables():
            select = (f'SELECT "id", {namestr} FROM'
                      f' (SELECT s.id, q.query_id FROM "{sco_type}" AS s'
                      f'  INNER JOIN __queries AS q ON s.id = q.sco_id'
                      f'  WHERE {where}) AS foo;')

            # Insert into membership table
            stmt = 'INSERT INTO __membership ("sco_id", "var") ' + select
            cursor = self._execute(stmt, cursor)

        # Create query for the view
        select = (f'SELECT * FROM "{sco_type}" WHERE "id" IN'
                  f' (SELECT "sco_id" FROM __membership'
                  f"  WHERE var = '{viewname}');")

        try:
            cursor = self._create_view(viewname, select, sco_type, deps=[tablename], cursor=cursor)
        except IncompatibleType:
            raise IncompatibleType(f'{viewname} has type "{old_type}"; cannot assign type "{sco_type}"')
        self.connection.commit()
        cursor.close()

    def _get_excluded(self, colnames, tablename):
        excluded = []
        for col in colnames:
            if col == 'first_observed':
                excluded.append(f'first_observed = {self.text_min}("{tablename}".first_observed, EXCLUDED.first_observed)')
            elif col == 'last_observed':
                excluded.append(f'last_observed = {self.text_max}("{tablename}".last_observed, EXCLUDED.last_observed)')
            elif col == 'number_observed':
                excluded.append(f'number_observed = "{tablename}".number_observed + EXCLUDED.number_observed')
            elif col == 'id':
                continue
            else:
                excluded.append(f'"{col}" = EXCLUDED."{col}"')
        return ', '.join(excluded)

    def upsert(self, cursor, tablename, obj, query_id):
        colnames = obj.keys()
        excluded = self._get_excluded(colnames, tablename)
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([self.placeholder] * len(obj))
        stmt = (f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders})'
                f' ON CONFLICT (id) DO UPDATE SET {excluded};')
        values = tuple([str(orjson.dumps(value), 'utf-8')
                        if isinstance(value, list) else value for value in obj.values()])
        logger.debug('_upsert: "%s"', stmt)
        cursor.execute(stmt, values)

        # Now add to query table as well
        stmt = (f'INSERT INTO "__queries" (sco_id, query_id)'
                f' VALUES ({self.placeholder}, {self.placeholder})')
        cursor.execute(stmt, (obj['id'], query_id))

    def upsert_many(self, cursor, tablename, objs, query_id):
        for obj in objs:
            self.upsert(cursor, tablename, obj, query_id)

    def cache(self, query_id, bundles, batchsize=2000):
        """Cache the result of a query"""
        logger.debug('Caching %s', query_id)

        if not isinstance(bundles, list):
            bundles = [bundles]

        writer = self._get_writer(str(query_id))
        splitter = SplitWriter(writer, batchsize=batchsize, query_id=str(query_id))

        # walk the bundles and figure out all the columns
        for bundle in bundles:
            if isinstance(bundle, str):
                logger.debug('- Caching %s', bundle)
            for obj in _transform(STIX_TRANSFORMS, bundle):
                splitter.write(obj)
        splitter.close()

    def assign(self, viewname, on, op=None, by=None, ascending=True, limit=None):
        """
        Perform (unary) operation `op` on `on` and store result as `viewname`
        """
        validate_name(viewname)
        if by:
            validate_path(by)
            _, _, by = by.rpartition(':')
        if op == 'sort':
            stmt = self._select(
                on, sortby=by, ascending=ascending, limit=limit)
        elif op == 'group':
            stmt = self._select(on, groupby=by)
        sco_type = self.table_type(on)
        cursor = self._create_view(viewname, stmt, sco_type, deps=[on])
        self.connection.commit()
        cursor.close()

    def load(self, viewname, objects, sco_type=None, query_id=None, preserve_ids=True):
        """Import `objects` as type `sco_type` and store as `viewname`"""
        validate_name(viewname)
        if not query_id:
            # Look inside data
            if 'query_id' in objects[0]:
                query_id = objects[0]['query_id']
            else:
                query_id = str(uuid.uuid4())
        writer = self._get_writer(query_id)
        splitter = SplitWriter(writer, batchsize=1000, query_id=str(query_id))

        for obj in objects:
            if not sco_type:
                # objects MUST be dicts with a type
                if 'type' not in obj:
                    raise InvalidObject('missing `type`')
                sco_type = obj['type']
            if isinstance(obj, str):
                obj = {'type': sco_type, primary_prop(sco_type): obj}
            elif not isinstance(obj, dict):
                raise InvalidObject('Unknown data format')
            if 'id' not in obj or not preserve_ids:
                obj['id'] = sco_type + '--' + str(uuid.uuid4())
            if 'type' not in obj:
                obj['type'] = sco_type
            splitter.write(obj)
        splitter.close()

        self.extract(viewname, sco_type, query_id, '')

        return sco_type

    def reassign(self, viewname, objects):
        """Replace `objects` (or insert them if they're not there)"""
        writer = self._get_writer(None)
        splitter = SplitWriter(writer, batchsize=1000, replace=True)
        sco_type = None
        for obj in objects:
            if 'type' not in obj:
                raise InvalidObject('missing `type`')
            elif not isinstance(obj, dict):
                raise InvalidObject('Unknown data format')
            if not sco_type:
                sco_type = obj['type']
            if 'id' not in obj:
                raise InvalidObject('missing `id`')
            splitter.write(obj)
        splitter.close()

        # If we're reassigning an existing viewname, we need to drop old membership
        namestr = f"'{viewname}'"
        cursor = self.connection.cursor()
        cursor.execute('BEGIN')
        if viewname in self.views():
            stmt = f'DELETE FROM __membership WHERE var = {namestr};'
            cursor = self._execute(stmt, cursor)

        # Insert into membership table
        for obj in objects:
            stmt = f'INSERT INTO __membership ("sco_id", "var") VALUES ({self.placeholder}, {self.placeholder});'
            cursor.execute(stmt, (obj['id'], viewname))

        # Create view
        select = (f'SELECT * FROM "{sco_type}" WHERE "id" IN'
                  f' (SELECT "sco_id" FROM __membership'
                  f"  WHERE var = '{viewname}');")
        cursor = self._create_view(viewname, select, sco_type, cursor=cursor)
        self.connection.commit()

    def update(self, objects, query_id=None):
        """Update `objects`"""
        writer = self._get_writer(None)
        splitter = SplitWriter(writer, batchsize=1000, replace=True)
        for obj in objects:
            if 'type' not in obj:
                raise InvalidObject('missing `type`')
            elif not isinstance(obj, dict):
                raise InvalidObject('Unknown data format')
            if 'id' not in obj:
                raise InvalidObject('missing `id`')
            splitter.write(obj)
        splitter.close()

    def join(self, viewname, l_var, l_on, r_var, r_on):
        """Join vars `l_var` and `r_var` and store result as `viewname`"""
        validate_name(viewname)
        validate_name(l_var)
        validate_name(r_var)
        validate_path(l_on)
        validate_path(r_on)
        l_cols = self.columns(l_var)
        r_cols = self.columns(r_var)
        l_type, _, l_on = l_on.rpartition(':')
        r_type, _, r_on = r_on.rpartition(':')
        cols = set()
        for col in l_cols:
            cols.add(f'{self.ifnull}({l_var}."{col}", {r_var}."{col}") AS "{col}"')
        for col in r_cols:
            # Only add if not already added from left
            if col not in l_cols:
                cols.add(f'{r_var}."{col}" as "{col}"')
        scols = ', '.join(cols)
        stmt = (f'SELECT {scols} FROM'
                f' {l_var} INNER JOIN {r_var}'
                f' ON {l_var}."{l_on}" = {r_var}."{r_on}"')
        sco_type = self.table_type(l_var)
        cursor = self._create_view(viewname, stmt, sco_type, deps=[l_var, r_var])
        self.connection.commit()
        cursor.close()

    def extract(self, viewname, sco_type, query_id, pattern):
        """
        Extract all `sco_type` object from the results of `query_id` and
        store as `viewname`

        """
        validate_name(viewname)
        logger.debug('Extract %s as %s from %s with %s',
                     sco_type, viewname, query_id, pattern)
        self._extract(viewname, sco_type, sco_type, pattern, query_id)

    def filter(self, viewname, sco_type, input_view, pattern):
        """
        Extract all `sco_type` object from `input_view` and store as
        `viewname`

        """
        validate_name(viewname)
        validate_name(input_view)
        logger.debug('Filter %s as %s from %s with %s',
                     sco_type, viewname, input_view, pattern)
        slct = self._get_view_def(input_view)
        try:
            where = stix2sql(pattern, sco_type) if pattern else None
        except Exception as e:
            logger.error('%s', e)
            raise StixPatternError(pattern) from e
        slct = f'SELECT * FROM ({slct}) AS tmp'
        if where:
            slct += f' WHERE {where}'
        cursor = self._create_view(viewname, slct, sco_type)
        self.connection.commit()
        cursor.close()

    def lookup(self, viewname, cols="*", limit=None, offset=None):
        """Get the value of `viewname`"""
        validate_name(viewname)
        if cols != "*":
            dbcols = self.columns(viewname)
            cols = cols.replace(" ", "").split(",")
            for col in cols:
                if col not in dbcols:
                    raise InvalidAttr(f"{col}")
        stmt = self._select(viewname, cols=cols, limit=limit, offset=offset)
        cursor = self._query(stmt)
        return cursor.fetchall()

    def values(self, path, viewname):
        """Get the values of STIX object path `path` (a column) from `viewname`"""
        validate_path(path)
        validate_name(viewname)
        _, _, column = path.rpartition(':')
        if column not in self.columns(viewname):
            raise InvalidAttr(path)
        stmt = f'SELECT "{column}" FROM "{viewname}"'
        cursor = self._query(stmt)
        result = cursor.fetchall()
        return [row[column] for row in result]

    def count(self, viewname):
        """Get the count of objects (rows) in `viewname`"""
        validate_name(viewname)
        stmt = f'SELECT COUNT(*) FROM "{viewname}"'
        cursor = self._query(stmt)
        res = cursor.fetchone()
        return list(res.values())[0] if res else 0

    def tables(self):
        """Get all table names"""
        # This is DB-specific
        raise NotImplementedError('Storage.tables')

    def views(self):
        """Get all view names"""
        # This is DB-specific
        raise NotImplementedError('Storage.views')

    def table_type(self, viewname):
        """Get the SCO type for table/view `viewname`"""
        validate_name(viewname)
        #stmt = f'SELECT "type" FROM "{viewname}" WHERE "type" IS NOT NULL LIMIT 1;'
        stmt = f'SELECT "type" FROM "__symtable" WHERE name = {self.placeholder};'
        cursor = self._query(stmt, (viewname,))
        res = cursor.fetchone()
        return list(res.values())[0] if res else None

    def columns(self, viewname):
        """Get the column names (properties) of `viewname`"""
        # This is DB-specific
        raise NotImplementedError('Storage.columns')

    def schema(self, viewname):
        """Get the schema (names and types) of `viewname`"""
        # This is DB-specific
        raise NotImplementedError('Storage.schema')

    def delete(self):
        """Delete ALL data in this store"""
        # This is DB-specific
        raise NotImplementedError('Storage.delete')

    def set_appdata(self, viewname, data):
        """Attach app-specific data to a viewname"""
        validate_name(viewname)
        stmt = (f'UPDATE "__symtable" SET appdata = {self.placeholder}'
                f' WHERE name = {self.placeholder};')
        values = (data, viewname)
        cursor = self._query(stmt, values=values)
        cursor.close()

    def get_appdata(self, viewname):
        """Retrieve app-specific data for a viewname"""
        validate_name(viewname)
        stmt = f'SELECT appdata FROM "__symtable" WHERE name = {self.placeholder};'
        values = (viewname,)
        cursor = self._query(stmt, values)
        res = cursor.fetchone()
        cursor.close()
        if not res:
            return None
        if 'appdata' in res:
            return res['appdata']
        return res[0]

    def get_view_data(self, viewnames=None):
        """Retrieve information about one or more viewnames"""
        if viewnames:
            placeholders = ', '.join([self.placeholder] * len(viewnames))
            stmt = f'SELECT * FROM "__symtable" WHERE name IN ({placeholders});'
            values = tuple(viewnames)
        else:
            stmt = 'SELECT * FROM "__symtable";'
            values = None
        cursor = self._query(stmt, values)
        res = cursor.fetchall()
        cursor.close()
        return res

    def run_query(self, query):
        query_text, query_values = query.render(self.placeholder)
        return self._query(query_text, query_values)

    def merge(self, viewname, input_views):
        validate_name(viewname)
        selects = []
        types = set()
        for name in input_views:
            validate_name(name)
            types.add(self.table_type(name))
            selects.append(self._get_view_def(name))
        if len(types) > 1:
            raise IncompatibleType('cannot merge types ' + ', '.join(types))
        stmt = ' UNION '.join(selects)
        sco_type = self.table_type(input_views[0])
        cursor = self._create_view(viewname, stmt, sco_type, deps=input_views)
        self.connection.commit()
        cursor.close()

    def remove_view(self, viewname):
        """Remove view `viewname`"""
        validate_name(viewname)
        cursor = self._execute('BEGIN;')
        self._execute(f'DROP VIEW IF EXISTS "{viewname}";', cursor)
        self._drop_name(cursor, viewname)
        self.connection.commit()
        cursor.close()

    def rename_view(self, oldname, newname):
        """Rename view `oldname` to `newname`"""
        validate_name(oldname)
        validate_name(newname)
        view_type = self.table_type(oldname)
        view_def = self._get_view_def(oldname)
        cursor = self._execute('BEGIN;')

        # Need to remove `newname` if it already exists
        self._execute(f'DROP VIEW IF EXISTS "{newname}";', cursor)
        self._drop_name(cursor, newname)

        # Update membership table
        self._execute(f"UPDATE __membership SET var = '{newname}' WHERE var = '{oldname}';", cursor)

        # Now do the rename
        qry = re.sub(f'var = \'{oldname}\'',  # This is an ugly hack
                     f'var = \'{newname}\'',
                     view_def)
        self._create_view(newname, qry, view_type, cursor=cursor)
        self._execute(f'DROP VIEW IF EXISTS "{oldname}"', cursor)
        self._drop_name(cursor, oldname)
        self._new_name(cursor, newname, view_type)

        self.connection.commit()
        cursor.close()
