import logging
import re
import uuid

import orjson
import ujson

from firepit import raft
from firepit.exceptions import IncompatibleType
from firepit.exceptions import InvalidAttr
from firepit.exceptions import InvalidObject
from firepit.exceptions import InvalidStixPath
from firepit.exceptions import StixPatternError
from firepit.exceptions import UnknownViewname
from firepit.props import auto_agg
from firepit.props import auto_agg_tuple
from firepit.props import parse_path
from firepit.props import primary_prop
from firepit.query import Aggregation
from firepit.query import Column
from firepit.query import Filter
from firepit.query import Group
from firepit.query import Join
from firepit.query import Limit
from firepit.query import Offset
from firepit.query import Order
from firepit.query import Predicate
from firepit.query import Projection
from firepit.query import Query
from firepit.query import Table
from firepit.splitter import SplitWriter
from firepit.stix20 import stix2sql
from firepit.stix21 import makeid
from firepit.validate import validate_name
from firepit.validate import validate_path

logger = logging.getLogger(__name__)


def _transform(filename):
    for obj in raft.get_objects(filename):  #, ['identity', 'observed-data']):
        if obj['type'] != 'identity':
            obj['x_stix'] = ujson.dumps(obj)  # raft.preserve
            for o in (raft.json_normalize(obj, flat_lists=False) for obj in raft.make_sro(obj)):
                yield o
        else:
            yield obj


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

        # Python-to-SQL type mapper
        self.infer_type = infer_type

    def _get_writer(self, **kwargs):
        """Get a DB inserter object"""
        # This is DB-specific
        raise NotImplementedError('SqlStorage._get_writer')

    def _initdb(self, cursor):
        """Do some initial DB setup"""
        stmt = ('CREATE TABLE IF NOT EXISTS "__symtable" '
                '(name TEXT, type TEXT, appdata TEXT);')
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
        if not cursor:
            cursor = self.connection.cursor()
        cursor.execute(cmd)
        self.connection.commit()

    def _query(self, query, values=None, cursor=None):
        """Private wrapper for logging SQL query"""
        logger.debug('Executing query: %s', query)
        if not cursor:
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

    def _create_index(self, tablename, cursor):
        if tablename in ['__contains', '__reflist', 'relationship']:
            for col in ['source_ref', 'target_ref']:
                self._execute(f'CREATE INDEX "{tablename}_{col}_idx" ON "{tablename}" ("{col}");', cursor)

    def _create_table(self, tablename, columns):
        stmt = f'CREATE TABLE "{tablename}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in columns.items()])
        stmt += ');'
        logger.debug('_create_table: "%s"', stmt)
        cursor = self._execute(stmt)
        self._create_index(tablename, cursor)
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

    def _is_sql_view(self, name, cursor=None):
        ## This is DB-specific
        raise NotImplementedError('Storage._is_sql_view')

    def path_joins(self, viewname, sco_type, column):
        """Determine if `column` has implicit Joins and return them if so"""
        if not sco_type:
            sco_type = self.table_type(viewname)
        aliases = {sco_type: viewname}
        links = parse_path(column)
        target_table = None
        target_column = None
        results = []  # Query components to return
        for link in links:
            if link[0] == 'node':
                if not target_table:
                    target_table = link[1] or viewname
                if not target_column:
                    target_column = link[2]
                else:
                    target_column += f'.{link[2]}'
            elif link[0] == 'rel':
                from_type = link[1] or viewname
                ref_name = link[2]
                if target_column:
                    target_column = None
                to_type = link[3]
                target_table = to_type
                lhs = aliases.get(from_type, from_type)
                alias, _, _ = ref_name.rpartition('_')
                aliases[to_type] = alias
                results.append(Join(to_type, ref_name, '=', 'id', lhs=lhs, alias=alias))
            target_table = aliases.get(target_table, target_table)
        return results, target_table, target_column

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
        cursor = self._execute('BEGIN;')
        select = (f'SELECT * FROM "{sco_type}" WHERE "id" IN'
                  f' (SELECT "{sco_type}".id FROM "{sco_type}"'
                  f'  INNER JOIN __queries ON "{sco_type}".id = __queries.sco_id'
                  f'  WHERE {where});')

        cursor = self._create_view(viewname, select, sco_type, deps=[tablename], cursor=cursor)
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

    def upsert(self, cursor, tablename, obj, query_id, schema):
        colnames = [k for k in list(schema.keys()) if k != 'type']
        excluded = self._get_excluded(colnames, tablename)
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([self.placeholder] * len(colnames))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders})'
        if 'id' in colnames:
            action = f'UPDATE SET {excluded}' if excluded else 'NOTHING'
            stmt += f' ON CONFLICT (id) DO {action}'
        values = tuple([str(orjson.dumps(value), 'utf-8')
                        if isinstance(value, list) else value for value in obj])
        #logger.debug('_upsert: obj=%s cols=%s schema=%s "%s", %s', obj, colnames, schema, stmt, values)
        cursor.execute(stmt, values)

        if query_id and 'id' in colnames:
            # Now add to query table as well
            idx = colnames.index('id')
            stmt = (f'INSERT INTO "__queries" (sco_id, query_id)'
                    f' VALUES ({self.placeholder}, {self.placeholder})')
            cursor.execute(stmt, (obj[idx], query_id))

    def upsert_many(self, cursor, tablename, objs, query_id, schema):
        for obj in objs:
            self.upsert(cursor, tablename, obj, query_id, schema)

    def cache(self, query_id, bundles, batchsize=2000, **kwargs):
        """Cache the result of a query/dataset

        Takes the `observed-data` SDOs from `bundles` and "flattens"
        them, splits out SCOs by type, and inserts into a database
        with 1 table per type.

        Accepts some keyword args for runtime options, some of which
        may depend on what database type is in use (e.g. sqlite3,
        postgresql, ...)

        Args:

          query_id (str): a unique identifier for this set of bundles

          bundles (list): STIX bundles (either in-memory Python objects or filename paths)

          batchsize (int): number of objects to insert in 1 batch (defaults to 2000)

        """
        logger.debug('Caching %s', query_id)

        if not isinstance(bundles, list):
            bundles = [bundles]

        writer = self._get_writer(**kwargs)
        splitter = SplitWriter(writer, batchsize=batchsize, query_id=str(query_id))

        # walk the bundles and figure out all the columns
        for bundle in bundles:
            if isinstance(bundle, str):
                logger.debug('- Caching %s', bundle)
            for obj in _transform(bundle):
                splitter.write(obj)
        splitter.close()

    def assign(self, viewname, on, op=None, by=None, ascending=True, limit=None):
        """
        DEPRECATED: Perform (unary) operation `op` on `on` and store result as `viewname`
        """
        validate_name(viewname)
        validate_name(on)
        query = Query(on)
        if by:
            validate_path(by)
            sco_type, _, by = by.rpartition(':')
            target_column = by
            if by not in self.columns(on):
                joins, _, target_column = self.path_joins(on, sco_type, by)
                query.extend(joins)
        if op == 'sort':
            query.append(Order([(target_column, Order.ASC if ascending else Order.DESC)]))
            if limit:
                query.append(Limit(limit))
            #query.append(Projection(self.columns(on)))  # Is this necessary?
            cols = [Column(c, table=on) for c in self.columns(on)]
            query.append(Projection(cols))  # Is this necessary?
        elif op == 'group':
            query.append(Group([Column(target_column, alias=by)]))
        self.assign_query(viewname, query)

    def load(self, viewname, objects, sco_type=None, query_id=None, preserve_ids=True):
        """Import `objects` as type `sco_type` and store as `viewname`"""
        validate_name(viewname)
        if not query_id:
            # Look inside data
            if 'query_id' in objects[0]:
                query_id = objects[0]['query_id']
            else:
                query_id = str(uuid.uuid4())
        writer = self._get_writer(query_id=query_id)
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
            if 'type' not in obj:
                obj['type'] = sco_type
            if 'id' not in obj or not preserve_ids:
                obj['id'] = makeid(obj)
            splitter.write(obj)
        splitter.close()

        self.extract(viewname, sco_type, query_id, '')

        return sco_type

    def reassign(self, viewname, objects):
        """Replace `objects` (or insert them if they're not there)"""
        validate_name(viewname)
        # TODO: ensure viewname exists?  Do we care?

        # Ignore it if objects is empty
        if not objects:
            return

        cursor = self._execute('BEGIN;')
        if 'id' not in objects[0]:
            # Maybe it's aggregates?  Do "copy-on-write"
            self._execute(f'DROP VIEW IF EXISTS "{viewname}"', cursor)
            columns = [key for key in objects[0].keys() if key != 'type']
            schema = {}
            for col in columns:
                schema[col] = self.infer_type(col, objects[0][col])
            self._create_table(viewname, schema)
            records = [[obj.get(col) for col in columns] for obj in objects]
            self.upsert_many(cursor, viewname, records, None, schema)
            viewdef = self._select(viewname)
        else:
            writer = self._get_writer()
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
            viewdef = self._get_view_def(viewname)
            self._execute(f'DROP VIEW IF EXISTS "{viewname}"', cursor)

            # Recreate view
            self._execute(f'CREATE VIEW "{viewname}" AS {viewdef}', cursor)

        self.connection.commit()

    def join(self, viewname, l_var, l_on, r_var, r_on):
        """Join vars `l_var` and `r_var` and store result as `viewname`"""
        validate_name(viewname)
        validate_name(l_var)
        validate_name(r_var)
        validate_path(l_on)
        validate_path(r_on)
        l_cols = set(self.columns(l_var))
        r_cols = set(self.columns(r_var))
        l_type, _, l_on = l_on.rpartition(':')
        r_type, _, r_on = r_on.rpartition(':')
        cols = set()
        for col in l_cols - r_cols:
            cols.add(f'{l_var}."{col}" AS "{col}"')
        for col in l_cols & r_cols:
            cols.add(f'{self.ifnull}({l_var}."{col}", {r_var}."{col}") AS "{col}"')
        for col in r_cols - l_cols:
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
        cursor = self._create_view(viewname, slct, sco_type, deps=[input_view])
        self.connection.commit()
        cursor.close()

    def lookup(self, viewname, cols="*", limit=None, offset=None):
        """Get the value of `viewname`"""
        qry = Query(viewname)
        if cols != "*":
            dbcols = self.columns(viewname)
            if isinstance(cols, str):
                cols = cols.replace(" ", "").split(",")
            proj = []
            for col in cols:
                if col not in dbcols:
                    try:
                        validate_path(col)
                    except InvalidStixPath as e:
                        raise InvalidAttr(f"{col}") from e
                    joins, target_table, target_column = self.path_joins(viewname, None, col)
                    qry.extend(joins)
                    proj.append(Column(target_column, table=target_table, alias=col))
                else:
                    proj.append(Column(col, viewname))
            qry.append(Projection(proj))
        if limit:
            qry.append(Limit(limit))
        if offset:
            qry.append(Offset(offset))
        sco_type = self.table_type(viewname) or viewname
        cursor = self.run_query(qry)
        results = cursor.fetchall()
        if 'type' in cols or cols == '*':
            for result in results:
                result['type'] = sco_type
        return results

    def values(self, path, viewname):
        """Get the values of STIX object path `path` (a column) from `viewname`"""
        validate_path(path)
        validate_name(viewname)
        sco_type, _, column = path.rpartition(':')
        qry = Query(viewname)
        if column not in self.columns(viewname):
            joins, target_table, target_column = self.path_joins(viewname, sco_type, column)
            qry.extend(joins)
            qry.append(Projection([Column(target_column, table=target_table, alias=column)]))
        else:
            qry.append(Projection([column]))
        cursor = self.run_query(qry)
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

    def types(self):
        """Get all table names that correspond to SCO types"""
        # This is DB-specific
        raise NotImplementedError('Storage.types')

    def views(self):
        """Get all view names"""
        stmt = 'SELECT name FROM __symtable'
        cursor = self._query(stmt)
        result = cursor.fetchall()
        return [row['name'] for row in result]

    def table_type(self, viewname):
        """Get the SCO type for table/view `viewname`"""
        validate_name(viewname)
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

    def finish(self, index=True):
        """Do any DB-specific post-caching/insertion activity, such as indexing"""
        # This is a DB-specific hook, but by default we'll do nothing
        pass

    def assign_query(self, viewname, query, sco_type=None):
        """
        Create a new view `viewname` defined by `query`
        """
        # Deduce SCO type and "deps" of viewname from query
        deps = []
        group_cols = []
        group_pos = None
        found_agg = False
        for i, stage in enumerate(query.stages):
            if isinstance(stage, Table):
                # Assume first Table?  Might not work for complex queries or joins
                on = stage.name
                if not sco_type:
                    sco_type = self.table_type(on)
                deps = [on]
            elif isinstance(stage, Aggregation):
                found_agg = True
            elif isinstance(stage, Group):
                group_cols = stage.cols
                group_pos = i

        # if no aggs supplied, do "auto aggregation"
        if group_cols and not found_agg and sco_type:
            group_colnames = {c.name if hasattr(c, 'name') else c for c in group_cols}
            aggs = []
            for col in self.schema(sco_type):  #viewname):
                # Don't aggregate the columns we used for grouping
                if col['name'] in group_colnames:
                    continue
                agg = auto_agg_tuple(sco_type, col['name'], col['type'])
                if agg:
                    aggs.append(agg)
            agg = Aggregation(aggs)
            agg.group_cols = group_cols  #FIXME: how to alias?  Maybe table stack?
            query.stages.insert(group_pos, agg)  # Nasty

        query_text, query_values = query.render('{}')
        formatted_values = [f"'{v}'" if isinstance(v, str) else v for v in query_values]
        stmt = query_text.format(*formatted_values)
        logger.debug('assign_query: %s', stmt)
        cursor = self._create_view(viewname, stmt, sco_type, deps=deps)
        self.connection.commit()
        cursor.close()

    def value_counts(self, viewname, path):
        """
        Get the count of observations of each value in `viewname`.`path`
        Returns list of dicts like {'{column}': '...', 'count': 1}
        """
        validate_name(viewname)
        _, _, column = path.rpartition(':')

        qry = Query([
            Table(viewname),
            Join('__contains', 'id', '=', 'target_ref'),
            Join('observed-data', 'source_ref', '=', 'id'),
            Group([column]),
            Aggregation([('COUNT', '*', 'count')])
        ])
        cursor = self.run_query(qry)
        return cursor.fetchall()

    def number_observed(self, viewname, path, value=None):
        """
        Get the count of observations of `value` in `viewname`.`path`
        Returns integer count
        """
        qry = Query([
            Table(viewname),
            Join('__contains', 'id', '=', 'target_ref'),
            Join('observed-data', 'source_ref', '=', 'id')
        ])
        joins, _, col = self.path_joins(viewname, None, path)
        qry.extend(joins)
        if value:
            qry.append(Filter([Predicate(col, '=', value)]))
        qry.append(Aggregation([('SUM', 'number_observed', 'count')]))
        cursor = self.run_query(qry)
        res = cursor.fetchone()
        count = res['count'] if res else 0
        return int(count) if count else 0

    def timestamped(self, viewname, path=None, value=None, timestamp='first_observed'):
        """
        Get the timestamped observations of `value` in `viewname`.`path`
        Returns list of dicts like {'timestamp': '2021-10-...', '{column}': '...'}
        """

        # Something like this:
        # select sco."{column}" as "{column}", obs."{ts}" as "{ts}"
        #   from "{viewname}" sco
        #     join __contains c on sco.id = c.target_ref
        #     join "observed-data" obs on c.source_ref = obs.id
        #   where sco."{column}" = {value};

        qry = Query([
            Table(viewname),
            Join('__contains', 'id', '=', 'target_ref'),
            Join('observed-data', 'source_ref', '=', 'id')
        ])
        table = viewname
        column = path
        if path:
            joins, table, column = self.path_joins(viewname, None, path)
            qry.extend(joins)
        if column and value is not None:
            qry.append(Filter([Predicate(column, '=', value)]))
        qry.append(Order([timestamp]))
        if column:
            qry.append(
                Projection([
                    Column(timestamp, 'observed-data'),
                    Column(column, table)
                ])
            )
        else:
            qry.append(
                Projection([
                    Column(timestamp, 'observed-data'),
                    Column('*', table)
                ])
            )
        cursor = self.run_query(qry)
        res = cursor.fetchall()
        cursor.close()
        return res

    def summary(self, viewname, path=None, value=None):
        """
        Get the first and last observed time and number observed for observations of `viewname`, optionally specifying `path` and `value`.
        Returns list of dicts like {'first_observed': '2021-10-...', 'last_observed': '2021-10-...', 'number_observed': N}
        """
        qry = Query([
            Table(viewname),
            Join('__contains', 'id', '=', 'target_ref'),
            Join('observed-data', 'source_ref', '=', 'id')
        ])
        table = viewname
        column = path
        if path:
            joins, table, column = self.path_joins(viewname, None, path)
            qry.extend(joins)
        if column and value is not None:
            qry.append(Filter([Predicate(column, '=', value)]))
        qry.append(
            Aggregation([
                ('MIN', 'first_observed', 'first_observed'),
                ('MAX', 'last_observed', 'last_observed'),
                ('SUM', 'number_observed', 'number_observed'),
            ])
        )
        try:
            cursor = self.run_query(qry)
            res = cursor.fetchone()
            cursor.close()
        except UnknownViewname as e:
            # Probably __contains, if no observed-data has been loaded
            logger.warning('Missing table: %s', e)
            res = None
        except InvalidAttr:
            # Probably a "grouped"/aggregate POD table (no id)
            res = None
        if not res:
            c = self.count(viewname)
            res = {'first_observed': None, 'last_observed': None, 'number_observed': c}
        return res

    def group(self, newname, viewname, by, aggs=None):
        """Create new view `newname` defined by grouping `viewname` by `by`"""
        if isinstance(by, str):
            by = [by]
        columns = self.columns(viewname)
        group_colnames = []
        qry = Query(viewname)
        for col in by:
            if col not in columns:
                joins, table, colname = self.path_joins(viewname, None, col)
                group_colnames.append(Column(colname, table, col))
                qry.extend(joins)
            else:
                group_colnames.append(Column(col, viewname))
        qry.append(Group(group_colnames))
        if not aggs:
            aggs = []
            sco_type = self.table_type(viewname)
            for col in self.schema(sco_type):
                # Don't aggregate the columns we used for grouping
                if col['name'] in group_colnames:
                    continue
                agg = auto_agg_tuple(sco_type, col['name'], col['type'])
                if agg:
                    aggs.append(agg)
        qry.append(Aggregation(aggs))
        self.assign_query(newname, qry)
