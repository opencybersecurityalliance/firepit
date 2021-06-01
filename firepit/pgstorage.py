import logging
import os
import random
import re
import string

import psycopg2
import psycopg2.extras

from firepit.splitter import SqlWriter
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import validate_name

logger = logging.getLogger(__name__)


def get_storage(url, session_id):
    dbname = url.path.lstrip('/')
    return PgStorage(dbname, url.geturl(), session_id)


def _infer_type(key, value):
    if key == 'id':
        rtype = 'TEXT UNIQUE'
    elif isinstance(value, bool):
        rtype = 'BOOLEAN'
    elif isinstance(value, int):
        rtype = 'INTEGER'
    elif isinstance(value, float):
        rtype = 'REAL'
    elif isinstance(value, list):
        rtype = 'TEXT'
    else:
        rtype = 'TEXT'
    return rtype


class PgStorage(SqlStorage):
    def __init__(self, dbname, url, session_id=None):
        super().__init__()
        self.placeholder = '%s'
        self.text_min = 'LEAST'
        self.text_max = 'GREATEST'
        self.ifnull = 'COALESCE'
        self.dbname = dbname
        if not session_id:
            session_id = 'firepit'
        self.session_id = session_id
        options = f'options=--search-path%3D{session_id}'
        sep = '&' if '?' in url else '?'
        connstring = f'{url}{sep}{options}'
        self.connection = psycopg2.connect(
            connstring,
            cursor_factory=psycopg2.extras.RealDictCursor)
        cursor = self.connection.cursor()

        if session_id:
            self._execute(f'CREATE SCHEMA IF NOT EXISTS "{session_id}";', cursor=cursor)
            self._execute(f'SET search_path TO "{session_id}";', cursor=cursor)

        try:
            self._execute('''CREATE FUNCTION match(pattern TEXT, value TEXT)
                RETURNS boolean AS $$
                    SELECT regexp_match(value, pattern) != '{}'
            $$ LANGUAGE SQL;''', cursor=cursor)
        except psycopg2.errors.DuplicateFunction:
            self.connection.rollback()

        try:
            self._execute('''CREATE FUNCTION in_subnet(addr TEXT, net TEXT)
                RETURNS boolean AS $$
                    SELECT addr::inet <<= net::inet;
            $$ LANGUAGE SQL;''', cursor=cursor)
        except psycopg2.errors.DuplicateFunction:
            self.connection.rollback()

        # Do DB initization
        self._initdb(cursor)  # This commits
        cursor.close()

        logger.debug("Connection to PostgreSQL DB %s successful", dbname)

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

    def _create_empty_view(self, viewname, cursor):
        cursor.execute(f'CREATE VIEW "{viewname}" AS SELECT NULL as type WHERE 1<>1;')

    def _create_view(self, viewname, select, deps=None, cursor=None):
        """Overrides parent"""
        validate_name(viewname)
        if not cursor:
            cursor = self._execute('BEGIN;')
        tmp = None
        if not deps:
            deps = []
        elif viewname in deps:
            # Rename old view to random var
            tmp = ''.join(random.choice(string.ascii_lowercase)
                          for x in range(8))
            self._execute(f'DROP VIEW IF EXISTS "{tmp}"', cursor)
            slct = self._get_view_def(viewname)
            self._create_view(tmp, slct, cursor=cursor)
            select = re.sub(f'"{viewname}"', tmp, select)
        try:
            self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {select};', cursor)
        except psycopg2.errors.UndefinedTable:
            self.connection.rollback()
            cursor = self._execute('BEGIN;')
            self._create_empty_view(viewname, cursor)
        if tmp:
            self._execute(f'DROP VIEW IF EXISTS "{tmp}"', cursor)
        self._new_name(cursor, viewname)
        return cursor

    def _get_view_def(self, viewname):
        cursor = self._query("SELECT definition"
                             " FROM pg_views"
                             " WHERE schemaname = %s"
                             " AND viewname = %s", (self.session_id, viewname))
        viewdef = cursor.fetchone()
        stmt = viewdef['definition'].rstrip(';')

        # PostgreSQL will "expand" the original "*" to the columns
        # that existed at that time.  We need to get the star back, to
        # match SQLite3's behavior.
        # Actually, I'm not convinced we need this...
        # return re.sub(r'^.*?FROM', 'SELECT * FROM', stmt)
        return stmt

    def tables(self):
        cursor = self._query("SELECT table_name"
                             " FROM information_schema.tables"
                             " WHERE table_schema = %s", (self.session_id, ))
        rows = cursor.fetchall()
        return [i['table_name'] for i in rows
                if not i['table_name'].startswith('__')]

    def views(self):
        cursor = self._query("SELECT table_name"
                             " FROM information_schema.tables"
                             " WHERE table_schema = %s"
                             " AND table_type = 'VIEW'", (self.session_id, ))
        rows = cursor.fetchall()
        return [i['table_name'] for i in rows]

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
