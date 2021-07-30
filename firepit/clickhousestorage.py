import logging
import os
import re
import time
import traceback

import orjson
import psycopg2
import psycopg2.extras

from firepit.exceptions import IncompatibleType
from firepit.exceptions import InvalidAttr
from firepit.exceptions import UnknownViewname
from firepit.splitter import SqlWriter
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import validate_name
from firepit.query import Table
from firepit.query import Join
import jaydebeapi

from firepit.stix20 import stix2sql

from firepit.props import auto_agg
from firepit.props import primary_prop
from firepit.validate import validate_name
from firepit.validate import validate_path


logger = logging.getLogger(__name__)


def get_storage(url, session_id):
    dbname = url.path.lstrip('/')
    return ClickhouseStorage(dbname, url.geturl(), session_id)


def _infer_type(key, value):
    if key == 'id':
        rtype = 'String'
    elif isinstance(value, bool):
        rtype = 'UInt8'
    elif isinstance(value, int):
        rtype = 'Int32'
    elif isinstance(value, float):
        rtype = 'Float32'
    elif isinstance(value, list):
        rtype = 'String'
    else:
        rtype = 'String'
    return rtype



class ConnectionWrapper():
    def __init__(self, connection):
         self.connection = connection

    def cursor(self):
        cursor =self.connection.cursor()
        return CursorWrapper(cursor);

    def close(self):
        if not self.connection._closed:
            self.connection.close()

    def commit(self):
        self.connection.commit()

    def _closed(self):
        return self.connection._closed

class CursorWrapper():
    def __init__(self, cursor,defaultDB=None):
         self.cursor = cursor
         #self.defaultDB = defaultDB

    def execute(self,query_text,query_values=None):
        #if self.defaultDB:
        #    self.cursor.execute(f"USE \"{self.defaultDB}\"")
        if query_text.lower()=='begin' or query_text.lower()=='begin;' or query_text.lower()=='commit' or query_text.lower()=='commit;':
            return
        else:
            index=0
            while query_text.find("?") and not query_values==None and index<len(query_values):
                value =  query_values[index]
                if isinstance(value, str):
                    value=f"'{value}'"
                query_text = query_text.replace("?",value,1)
                index = index+1
            return self.cursor.execute(query_text)

    def fetchall(self):
        convertedResult = []
        if self.cursor==None:
            return convertedResult;
        result =  self.cursor.fetchall()
        if result == None:
            return convertedResult;
        for row in result:
            convertedRow={}
            for index in range(0,len(self.cursor.description)):
                # Check for java numeric types being returned
                if not isinstance(row[index], str) and row[index] is not None and str(row[index]).isnumeric():
                    convertedRow[self.cursor.description[index][0]]=int(str(row[index]))
                else:
                    convertedRow[self.cursor.description[index][0]]=row[index]

            convertedResult.append(convertedRow)
        return convertedResult

    def fetchone(self):
        convertedResult = {}
        if self.cursor==None:
            return convertedResult;
        result =  self.cursor.fetchone()
        if result == None:
            return convertedResult;
        for index in range(0,len(self.cursor.description)):
            if not isinstance(result[index], str) and result[index] is not None and str(result[index]).isnumeric():
                convertedResult[self.cursor.description[index][0]]=int(str(result[index]))
            else:
                convertedResult[self.cursor.description[index][0]]=result[index]
        return convertedResult

    def close(self):
        self.cursor.close()


"""
clickhouse storage implementation for Firepit
url pattern:
    clickhouse://<clickhouse_url>:<clickhouse_port>/?user=<clickhouse_user>&password=<clickhouse_password>
"""
class ClickhouseStorage(SqlStorage):
    def __init__(self, dbname, url, session_id=None):
        super().__init__()
        logger.debug("Initializing Clickhouse Storage")
        self.placeholder = '?'
        self.dbname = dbname
        if not session_id:
            session_id = 'firepit'
        self.session_id = session_id
        self.connection = ConnectionWrapper(jaydebeapi.connect(
             "ru.yandex.clickhouse.ClickHouseDriver",
             f"jdbc:{url}",
             {'session_id':f'{session_id}'}
             ))
        cursor=self.connection.cursor()
        self.db_schema_prefix = f'"{self.session_id}".'
        self._execute(f'CREATE DATABASE IF NOT EXISTS "{self.session_id}";', cursor=cursor)

        # Do DB initization from base class
        stmt = (f'CREATE  TABLE IF NOT EXISTS {self.db_schema_prefix}"__symtable" '
                '(name String, type String, appdata String) ENGINE=MergeTree() primary key tuple();')
        self._execute(stmt, cursor)
        stmt = (f'CREATE TABLE IF NOT EXISTS {self.db_schema_prefix}"__membership" '
                '(sco_id String, var String) ENGINE=MergeTree() primary key tuple();')
        self._execute(stmt, cursor)
        stmt = (f'CREATE TABLE IF NOT EXISTS {self.db_schema_prefix}"__queries" '
                '(sco_id String, query_id String) ENGINE=MergeTree() primary key tuple();')
        self._execute(stmt, cursor)

        cursor.close()
        logger.debug("Connection to Clickhouse DB %s successful", dbname)

    def _get_writer(self, prefix):
        """Get a DB inserter object"""
        filedir = os.path.dirname(self.dbname)
        return SqlWriter(
            filedir,
            self,
            placeholder=self.placeholder,
            infer_type=_infer_type)

    def __del__(self):
        logger.debug("Closing Clickhouse DB connection")
        try:
            self.connection.close()
        except:
            pass

    def table_type(self, viewname):
        """Get the SCO type for table/view `viewname`"""
        validate_name(viewname)
        stmt = f'SELECT "type" FROM {self.db_schema_prefix}"__symtable" WHERE name = \'{viewname}\''
        cursor = self._query(stmt)
        res = cursor.fetchone()
        return res["type"] if res else None

    def rename_view(self, oldname, newname):
        """Rename view `oldname` to `newname`"""
        validate_name(oldname)
        validate_name(newname)
        try:
            view_type = self.table_type(oldname)
            cursor = self.connection.cursor()
            self._execute(f'RENAME TABLE {self.db_schema_prefix}"{oldname}"  TO {self.db_schema_prefix}"{newname}"', cursor)
            self._drop_name(cursor, oldname)
            self._new_name(cursor, newname, view_type)
            cursor.close()
        except:

            pass

    def lookup(self, viewname, cols="*", limit=None, offset=None):
        """Get the value of `viewname`"""
        validate_name(viewname)
        if cols != "*":
            dbcols = self.columns(viewname)
            cols = cols.replace(" ", "").split(",")
            for col in cols:
                if col not in dbcols:
                    raise InvalidAttr(f"{col}")
        try:
            stmt = self._select(f'{viewname}', cols=cols, limit=limit, offset=offset)


            cursor = self._query(stmt)
            result =cursor.fetchall()

            return result
        except Exception:
                return []


    def run_query(self, query):

        for i in range(0,len(query.stages)):
            if isinstance(query.stages[i], Table):
                query.stages[i].name = f'{self.session_id}"."{query.stages[i].name}'
            elif isinstance(query.stages[i], Join):
                query.stages[i].name = f'{self.session_id}"."{query.stages[i].name}'
                query.stages[i].prev_name = f'{self.session_id}"."{query.stages[i].prev_name}'

        query_text, query_values = query.render(self.placeholder)
        try:
            return self._query(query_text, query_values)
        except Exception:
            return CursorWrapper(None)

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
        stmt = ' UNION DISTINCT '.join(selects)
        sco_type = self.table_type(input_views[0])
        self._create_view(viewname, stmt, sco_type, deps=input_views)


    def _select(self, tvname, cols="*", sortby=None, groupby=None,
                ascending=True, limit=None, offset=None, where=None):
        """Generate a SELECT query on table or view `tvname`"""
        # TODO: Deprecate this in favor of query module
        #validate_name(tvname)
        if cols != "*":
            cols = ", ".join([f'"{col}"' if not col.startswith("'") else col for col in cols])

        stmt = f'SELECT {cols} FROM {self.db_schema_prefix}"{tvname}"'
        if where:
            stmt += f' WHERE {where}'
        if groupby:
            #validate_path(groupby)
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
            stmt = f'SELECT {group_cols} from {self.db_schema_prefix}"{tvname}"'
            stmt += f' GROUP BY "{groupby}"'
        if sortby:
            #validate_path(sortby)
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

    def _query(self, query, values=None):
        """Private wrapper for logging SQL query"""
        logger.debug('Executing query: %s', query)
        cursor = self.connection.cursor()
        cursor.execute(query,values)
        return cursor

    def _create_table(self, tablename, columns):
        # Same as base class, but disable WAL
        stmt = f'CREATE TABLE {self.db_schema_prefix}"{tablename}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in columns.items()])
        stmt += ') ENGINE=MergeTree() primary key tuple();'
        logger.debug('_create_table: "%s"', stmt)
        cursor = self._execute(stmt)
        if 'x_contained_by_ref' in columns:
            #self._execute(f'CREATE INDEX {self.db_schema_prefix}"{tablename}_obs" ON "{tablename}" ("x_contained_by_ref");', cursor)
            pass
        self.connection.commit()
        cursor.close()

    def _add_column(self, tablename, prop_name, prop_type):
        stmt = f'ALTER TABLE {self.db_schema_prefix}"{tablename}" ADD COLUMN "{prop_name}" {prop_type};'
        logger.debug('new_property: "%s"', stmt)
        cursor = self._execute(stmt)
        cursor.close()

    def _create_empty_view(self, viewname, cursor):
        cursor.execute(f'CREATE VIEW {self.db_schema_prefix}"{viewname}" AS SELECT NULL as type WHERE 1<>1;')

    def _create_view(self, viewname, select, sco_type, deps=None, cursor=None):
        """Overrides parent"""
        validate_name(viewname)
        if not cursor:
            cursor = self.connection.cursor()
        if not deps:
            deps = []
        elif viewname in deps:
            # Get the query that makes up the current view
            slct = self._get_view_def(viewname)
            self._execute(f'DROP VIEW IF EXISTS {self.db_schema_prefix}"{viewname}"', cursor)
            # Swap out the viewname for its definition
            select = re.sub(f'"{viewname}"', f'({slct}) AS tmp', select)
        try:
            cursor = self._execute(f'CREATE OR REPLACE VIEW {self.db_schema_prefix}"{viewname}" AS {select}', cursor)
        except Exception:
            #Ignore failure to create View
            #traceback.print_stack()
            return self.connection.cursor()
        self._new_name(cursor, viewname, sco_type)
        return cursor;


    def _new_name(self, cursor, name, sco_type):
        stmt = (f'INSERT INTO {self.db_schema_prefix}"__symtable" (name, type)'
                f' VALUES (\'{name}\', \'{sco_type}\');')
        cursor.execute(stmt)

    def _drop_name(self, cursor, name):
        stmt = f'ALTER TABLE {self.db_schema_prefix}"__symtable" DELETE WHERE name = \'{name}\';'
        cursor.execute(stmt)

    def _get_view_def(self, viewname):
        cursor = self._query("SELECT create_table_query"
                             " FROM system.tables"
                             " WHERE database = '%s'"
                             " AND name = '%s'"%(self.session_id, viewname))
        viewdef = cursor.fetchone()
        stmt = viewdef['create_table_query'].rstrip(';')
        # PostgreSQL will "expand" the original "*" to the columns
        # that existed at that time.  We need to get the star back, to
        # match SQLite3's behavior.
        return re.sub(r'^.*?FROM', 'SELECT * FROM', stmt, 1, re.DOTALL)

    def tables(self):
        cursor = self._query("SELECT name"
                             " FROM system.tables"
                             " WHERE database = '%s'"%(self.session_id))
        rows = cursor.fetchall()
        return [i['name'] for i in rows
                if not i['name'].startswith('__')]

    def views(self):
        cursor = self._query("SELECT name"
                             " FROM system.tables"
                             " WHERE database = '%s'"
                             " AND engine = 'View'"%(self.session_id ))
        rows = cursor.fetchall()
        return [i['name'] for i in rows]

    def columns(self, viewname):
        validate_name(viewname)
        cursor = self._query("SELECT name"
                             " FROM system.columns"
                             " WHERE database = '%s'"
                             " AND table = '%s'"%(self.session_id, viewname))
        mappings = cursor.fetchall()
        if mappings:
            result = [e["name"] for e in mappings]
        else:
            result = []
        return result

    def schema(self, viewname):
        validate_name(viewname)
        cursor = self._query("SELECT name AS name, type AS type"
                             " FROM system.columns"
                             " WHERE database = '%s'"
                             " AND table = '%s'"%(self.session_id, viewname))
        return [{k: v for k, v in row.items() if k in ['name', 'type']}
                for row in cursor.fetchall()]

    def delete(self):
        """Delete ALL data in this store"""
        self._execute(f'DROP DATABASE "{self.session_id}" CASCADE;', cursor)
        cursor.close()

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
        cursor = self.connection.cursor()

        # If we're reassigning an existing viewname, we need to drop old membership
        old_type = None
        if viewname in self.views():
            old_type = self.table_type(viewname)
            stmt = f'ALTER TABLE {self.db_schema_prefix}__membership DELETE WHERE var = {namestr};'
            cursor = self._execute(stmt, cursor)

        if tablename in self.tables():
            select = (f'SELECT "id", {namestr} FROM'
                      f' (SELECT s.id, q.query_id FROM {self.db_schema_prefix}"{sco_type}" AS s'
                      f'  INNER JOIN {self.db_schema_prefix}__queries AS q ON s.id = q.sco_id'
                      f'  WHERE {where}) AS foo;')

            # Insert into membership table
            stmt = f'INSERT INTO {self.db_schema_prefix}__membership ("sco_id", "var") ' + select
            cursor = self._execute(stmt, cursor)

        # Create query for the view
        select = (f'SELECT * FROM {self.db_schema_prefix}"{sco_type}" WHERE "id" IN'
                  f' (SELECT "sco_id" FROM {self.db_schema_prefix}__membership'
                  f"  WHERE var = '{viewname}');")

        try:
            self._create_view(viewname, select, sco_type, deps=[tablename], cursor=cursor)
        except IncompatibleType:
            raise IncompatibleType(f'{viewname} has type "{old_type}"; cannot assign type "{sco_type}"')

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
        if viewname in self.views():
            stmt = f'DELETE FROM {self.db_schema_prefix}__membership WHERE var = {namestr};'
            cursor = self._execute(stmt, cursor)

        # Insert into membership table
        for obj in objects:
            stmt = f'INSERT INTO {self.db_schema_prefix}__membership ("sco_id", "var") VALUES ({self.placeholder}, {self.placeholder});'
            cursor.execute(stmt, (obj['id'], viewname))

        # Create view
        select = (f'SELECT * FROM {self.db_schema_prefix}"{sco_type}" WHERE "id" IN'
                  f' (SELECT "sco_id" FROM {self.db_schema_prefix}__membership'
                  f"  WHERE var = '{viewname}');")
        cursor = self._create_view(viewname, select, sco_type, cursor=cursor)

    def get_view_data(self, viewnames=None):
        """Retrieve information about one or more viewnames"""
        if viewnames:
            placeholders = ', '.join([self.placeholder] * len(viewnames))
            views = ', '.join(f'"{w}"' for w in viewnames)
            stmt = f'SELECT * FROM {self.db_schema_prefix}"__symtable" WHERE name IN ({views})'
            values = tuple(viewnames)
        else:
            stmt = f'SELECT * FROM {self.db_schema_prefix}"__symtable";'
            values = None

        cursor = self._query(stmt)
        res = cursor.fetchall()
        cursor.close()
        return res

    def count(self, viewname):
        """Get the count of objects (rows) in `viewname`"""
        validate_name(viewname)
        stmt = f'SELECT COUNT(*) FROM {self.db_schema_prefix}"{viewname}"'
        try:
            cursor = self._query(stmt)
            res = cursor.fetchone()
            return res["count()"] if res else 0
        except Exception:
            return 0

    def remove_view(self, viewname):
        """Remove view `viewname`"""
        validate_name(viewname)
        cursor = self.connection.cursor()
        self._execute(f'DROP VIEW IF EXISTS {self.db_schema_prefix}"{viewname}";', cursor)
        self._drop_name(cursor, viewname)
        cursor.close()

    def upsert_many(self, cursor, tablename, objs, query_id):
        cols = set()
        for obj in objs:
            cols = cols.union(obj.keys())
        colnames = list(cols)
        if tablename == 'identity':
            action = f'NOTHING'
        else:
            excluded = self._get_excluded(colnames, tablename)
            action = f'UPDATE SET {excluded}'
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([f"({', '.join([self.placeholder] * len(colnames))})"] * len(objs))

        # Workaround because of problem executing prepared statements with placeholders
        values = []
        query_values = []
        valuesString=""
        queryValuesString=""
        for obj in objs:
            if len(valuesString)>0:
                valuesString="%s,"%valuesString
            if len(queryValuesString)>0:
                queryValuesString="%s,"%queryValuesString

            valuesString="%s ("%valuesString
            query_values.append(obj['id'])
            query_values.append(query_id)
            queryValuesString = "%s ('%s','%s')"%(queryValuesString,obj['id'],query_id)

            entry =""
            for c in colnames:
                if len(entry)>0:
                    entry="%s,"%entry
                value = obj.get(c, None)
                if isinstance(value, list) or isinstance(value, str):
                    entry="%s '%s'"%(entry,str(orjson.dumps(value), 'utf-8') if isinstance(value, list) else value)
                elif value == None:
                    entry="%s NULL"%(entry)
                else:
                    entry="%s %s"%(entry,value)
                values.append(str(orjson.dumps(value), 'utf-8') if isinstance(value, list) else value)
            valuesString="%s %s)"%(valuesString,entry)



        stmt = (f'INSERT INTO {self.db_schema_prefix}"{tablename}" ({valnames}) VALUES {valuesString}')
        logger.debug('upsert_many: count=%d table=%s columns=%s action=%s"',
                     len(objs), tablename, valnames, action)
        self._execute(stmt)

        # Now add to query table as well
        stmt = (f'INSERT INTO {self.db_schema_prefix}"__queries" (sco_id, query_id)'
                f' VALUES {queryValuesString}')

        self._execute(stmt)
