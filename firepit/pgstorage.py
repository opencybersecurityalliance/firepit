import logging
import os
import re

import orjson
import psycopg2
import psycopg2.extras

from firepit.exceptions import DuplicateTable
from firepit.exceptions import InvalidAttr
from firepit.exceptions import UnknownViewname
from firepit.splitter import SqlWriter
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import infer_type
from firepit.sqlstorage import validate_name

logger = logging.getLogger(__name__)


def get_storage(url, session_id):
    dbname = url.path.lstrip('/')
    return PgStorage(dbname, url.geturl(), session_id)


def _infer_type(key, value):
    # PostgreSQL type specializations
    rtype = None
    if isinstance(value, bool):
        rtype = 'BOOLEAN'
    else:
        # Fall back to defaults
        rtype = infer_type(key, value)
    return rtype


class PgStorage(SqlStorage):
    def __init__(self, dbname, url, session_id=None):
        super().__init__()
        self.placeholder = '%s'
        self.text_min = 'LEAST'
        self.text_max = 'GREATEST'
        self.ifnull = 'COALESCE'
        self.dbname = dbname
        self.infer_type = _infer_type
        if not session_id:
            session_id = 'firepit'
        self.session_id = session_id
        options = f'options=--search-path%3D{session_id}'
        sep = '&' if '?' in url else '?'
        connstring = f'{url}{sep}{options}'
        self.connection = psycopg2.connect(
            connstring,
            cursor_factory=psycopg2.extras.RealDictCursor)

        if session_id:
            try:
                self._execute(f'CREATE SCHEMA IF NOT EXISTS "{session_id}";')
            except psycopg2.errors.UniqueViolation:
                self.connection.rollback()

        self._execute(f'SET search_path TO "{session_id}";')

        stmt = ("SELECT (EXISTS (SELECT *"
                " FROM INFORMATION_SCHEMA.TABLES"
                " WHERE TABLE_SCHEMA = %s"
                " AND  TABLE_NAME = '__queries'))")
        res = self._query(stmt, (session_id,)).fetchone()
        done = list(res.values())[0] if res else False
        if not done:
            self._setup()

        logger.debug("Connection to PostgreSQL DB %s successful", dbname)

    def _setup(self):
        cursor = self._execute('BEGIN;')
        try:
            self._execute('''CREATE FUNCTION match(pattern TEXT, value TEXT)
                RETURNS boolean AS $$
                    SELECT regexp_match(value, pattern) IS NOT NULL;
            $$ LANGUAGE SQL;''', cursor=cursor)
            self._execute('''CREATE FUNCTION in_subnet(addr TEXT, net TEXT)
                RETURNS boolean AS $$
                    SELECT addr::inet <<= net::inet;
            $$ LANGUAGE SQL;''', cursor=cursor)

            # Do DB initization from base class
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__symtable" '
                    '(name TEXT, type TEXT, appdata TEXT);')
            self._execute(stmt, cursor)
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__queries" '
                    '(sco_id TEXT, query_id TEXT);')
            self._execute(stmt, cursor)
            self.connection.commit()
            cursor.close()
        except psycopg2.errors.DuplicateFunction:
            self.connection.rollback()

    def _get_writer(self, prefix):
        """Get a DB inserter object"""
        filedir = os.path.dirname(self.dbname)
        return SqlWriter(
            filedir,
            self,
            placeholder=self.placeholder,
            infer_type=_infer_type)

    def __del__(self):
        if self.connection:
            logger.debug("Closing PostgreSQL DB connection")
            self.connection.close()

    def _query(self, query, values=None, cursor=None):
        """Private wrapper for logging SQL query"""
        logger.debug('Executing query: %s', query)
        if not cursor:
            cursor = self.connection.cursor()
        if not values:
            values = ()
        try:
            cursor.execute(query, values)
        except psycopg2.errors.UndefinedColumn as e:
            self.connection.rollback()
            raise InvalidAttr(str(e)) from e
        except psycopg2.errors.UndefinedTable as e:
            self.connection.rollback()
            raise UnknownViewname(str(e)) from e
        self.connection.commit()
        return cursor

    def _create_table(self, tablename, columns):
        # Same as base class, but disable WAL
        stmt = f'CREATE UNLOGGED TABLE "{tablename}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in columns.items()])
        stmt += ');'
        logger.debug('_create_table: "%s"', stmt)
        try:
            cursor = self._execute(stmt)
            if 'x_contained_by_ref' in columns:
                self._execute(f'CREATE INDEX "{tablename}_obs" ON "{tablename}" ("x_contained_by_ref");', cursor)
            self.connection.commit()
            cursor.close()
        except (psycopg2.errors.DuplicateTable,
                psycopg2.errors.DuplicateObject,
                psycopg2.errors.UniqueViolation) as e:
            self.connection.rollback()
            raise DuplicateTable(tablename) from e

    def _add_column(self, tablename, prop_name, prop_type):
        stmt = f'ALTER TABLE "{tablename}" ADD COLUMN "{prop_name}" {prop_type};'
        logger.debug('new_property: "%s"', stmt)
        try:
            cursor = self._execute(stmt)
            self.connection.commit()
            cursor.close()
        except psycopg2.errors.DuplicateColumn:
            self.connection.rollback()

    def _create_empty_view(self, viewname, cursor):
        cursor.execute(f'CREATE VIEW "{viewname}" AS SELECT NULL as type WHERE 1<>1;')

    def _create_view(self, viewname, select, sco_type, deps=None, cursor=None):
        """Overrides parent"""
        validate_name(viewname)
        if not cursor:
            cursor = self._execute('BEGIN;')
        is_new = True
        if not deps:
            deps = []
        elif viewname in deps:
            is_new = False
            # Get the query that makes up the current view
            slct = self._get_view_def(viewname)
            if self._is_sql_view(viewname, cursor):
                self._execute(f'DROP VIEW IF EXISTS "{viewname}"', cursor)
            else:
                self._execute(f'ALTER TABLE "{viewname}" RENAME TO "_{viewname}"', cursor)
                slct = slct.replace(viewname, f'_{viewname}')
            # Swap out the viewname for its definition
            select = re.sub(f'"{viewname}"', f'({slct}) AS tmp', select)
        try:
            self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {select}', cursor)
        except psycopg2.errors.UndefinedTable:
            # Missing dep?
            self.connection.rollback()
            cursor = self._execute('BEGIN;')
            self._create_empty_view(viewname, cursor)
        except psycopg2.errors.InvalidTableDefinition:
            # Usually "cannot drop columns from view"
            #logger.error(e, exc_info=e)
            self.connection.rollback()
            cursor = self._execute('BEGIN;')
            self._execute(f'DROP VIEW IF EXISTS "{viewname}";', cursor)
            self._execute(f'CREATE VIEW "{viewname}" AS {select}', cursor)
            is_new = False
        if is_new:
            self._new_name(cursor, viewname, sco_type)
        return cursor

    def _get_view_def(self, viewname):
        cursor = self._query("SELECT definition"
                             " FROM pg_views"
                             " WHERE schemaname = %s"
                             " AND viewname = %s", (self.session_id, viewname))
        viewdef = cursor.fetchone()
        if viewdef:
            stmt = viewdef['definition'].rstrip(';')

            # PostgreSQL will "expand" the original "*" to the columns
            # that existed at that time.  We need to get the star back, to
            # match SQLite3's behavior.
            return re.sub(r'^.*?FROM', 'SELECT * FROM', stmt, 1, re.DOTALL)

        # Must be a table
        return f'SELECT * FROM "{viewname}"'

    def _is_sql_view(self, name, cursor=None):
        cursor = self._query("SELECT definition"
                             " FROM pg_views"
                             " WHERE schemaname = %s"
                             " AND viewname = %s", (self.session_id, name))
        viewdef = cursor.fetchone()
        return viewdef is not None

    def tables(self):
        cursor = self._query("SELECT table_name"
                             " FROM information_schema.tables"
                             " WHERE table_schema = %s"
                             "   AND table_type != 'VIEW'", (self.session_id, ))
        rows = cursor.fetchall()
        return [i['table_name'] for i in rows
                if not i['table_name'].startswith('__')]

    def types(self):
        stmt = ("SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = %s AND table_type != 'VIEW'"
                "  EXCEPT SELECT name as table_name FROM __symtable")
        cursor = self._query(stmt, (self.session_id, ))
        rows = cursor.fetchall()
        # Ignore names that start with 1 or 2 underscores
        return [i['table_name'] for i in rows
                if not i['table_name'].startswith('_')]

    def columns(self, viewname):
        validate_name(viewname)
        cursor = self._query("SELECT column_name"
                             " FROM information_schema.columns"
                             " WHERE table_schema = %s"
                             " AND table_name = %s", (self.session_id, viewname))
        rows = cursor.fetchall()
        return [i['column_name'] for i in rows]

    def schema(self, viewname):
        validate_name(viewname)
        cursor = self._query("SELECT column_name AS name, data_type AS type"
                             " FROM information_schema.columns"
                             " WHERE table_schema = %s"
                             " AND table_name = %s", (self.session_id, viewname))
        return cursor.fetchall()

    def delete(self):
        """Delete ALL data in this store"""
        cursor = self._execute('BEGIN;')
        self._execute(f'DROP SCHEMA "{self.session_id}" CASCADE;', cursor)
        self.connection.commit()
        cursor.close()

    def upsert_many(self, cursor, tablename, objs, query_id):
        cols = set()
        for obj in objs:
            cols = cols.union(obj.keys())
        colnames = list(cols)
        if tablename == 'identity':
            action = 'NOTHING'
        else:
            excluded = self._get_excluded(colnames, tablename)
            action = f'UPDATE SET {excluded}'
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join([f"({', '.join([self.placeholder] * len(colnames))})"] * len(objs))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES {placeholders}'
        if 'id' in colnames:
            stmt += f' ON CONFLICT (id) DO {action}'
        values = []
        query_values = []
        for obj in objs:
            if query_id:
                query_values.append(obj['id'])
                query_values.append(query_id)
            for c in colnames:
                value = obj.get(c, None)
                values.append(str(orjson.dumps(value), 'utf-8') if isinstance(value, list) else value)
        logger.debug('upsert_many: count=%d table=%s columns=%s action=%s"',
                     len(objs), tablename, valnames, action)
        cursor.execute(stmt, values)

        if query_id:
            # Now add to query table as well
            placeholders = ', '.join([f'({self.placeholder}, {self.placeholder})'] * len(objs))
            stmt = (f'INSERT INTO "__queries" (sco_id, query_id)'
                    f' VALUES {placeholders}')
            cursor.execute(stmt, query_values)
