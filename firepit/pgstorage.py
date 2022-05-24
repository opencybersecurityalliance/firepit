import logging
import os
import re
from collections import defaultdict
from functools import lru_cache

import psycopg2
import psycopg2.extras
import ujson

from firepit.exceptions import DuplicateTable
from firepit.exceptions import InvalidAttr
from firepit.exceptions import UnexpectedError
from firepit.exceptions import UnknownViewname
from firepit.splitter import SqlWriter
from firepit.sqlstorage import DB_VERSION
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import infer_type
from firepit.sqlstorage import validate_name

logger = logging.getLogger(__name__)


MATCH_BIN = '''CREATE FUNCTION firepit_common.match_bin(pattern TEXT, value TEXT)
RETURNS boolean AS $$
    SELECT regexp_match(convert_from(decode(value, 'base64'), 'UTF8'), pattern) IS NOT NULL;
$$ LANGUAGE SQL;'''

LIKE_BIN = '''CREATE FUNCTION firepit_common.like_bin(pattern TEXT, value TEXT)
RETURNS boolean AS $$
    SELECT convert_from(decode(value, 'base64'), 'UTF8') LIKE pattern;
$$ LANGUAGE SQL;'''


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


# PostgreSQL defaults for COPY text format
SEP = '\t'
TEXT_ESCAPE_TABLE = str.maketrans({
    '\\': '\\\\',
    '\n': '\\n',
    '\r': '\\r',
    SEP: f'\\{SEP}'
})


@lru_cache(maxsize=256, typed=True)
def _text_encode(value):
    if value is None:
        return r'\N'
    elif not isinstance(value, str):
        return str(value)
    # MUST "escape" special chars
    return value.translate(TEXT_ESCAPE_TABLE)


class ListToTextIO:
    """
    Convert an iterable of lists into a file-like object with
    PostgreSQL TEXT formatting
    """

    def __init__(self, objs, cols, sep=SEP):
        self.it = iter(objs)
        self.cols = cols
        self.sep = sep
        self.buf = ''

    def read(self, n):
        result = ''
        try:
            while n > len(self.buf):
                obj = next(self.it)
                vals = [ujson.dumps(val, ensure_ascii=False) if isinstance(val, list)
                        else _text_encode(val) for val in obj]
                self.buf += self.sep.join(vals) + '\n'
            result = self.buf[:n]
            self.buf = self.buf[n:]
        except StopIteration:
            result = self.buf
            self.buf = ''
        return result


class TuplesToTextIO:
    """
    Convert an iterable of tuples into a file-like object
    """

    def __init__(self, objs, cols, sep=SEP):
        self.it = iter(objs)
        self.cols = cols
        self.sep = sep
        self.buf = ''

    def read(self, n):
        result = ''
        try:
            while n > len(self.buf):
                obj = next(self.it)
                self.buf += self.sep.join(obj)
                self.buf += '\n'
            result = self.buf[:n]
            self.buf = self.buf[n:]
        except StopIteration:
            result = self.buf
            self.buf = ''
        return result


def _rewrite_query(qry):
    parts = qry.split('UNION')
    new_parts = []
    for part in parts:
        new_parts.append(_rewrite_select(part).strip())
    return ' UNION '.join(new_parts)


def _rewrite_select(stmt):
    p = r"SELECT (DISTINCT )?(\"observed-data\".[\w_]+\W+)?(\"?[\w\d_-]+\"?\.\"?['\w\d\._-]+\"?,?\W+)+FROM"
    m = re.search(p, stmt)
    if m:
        matched = m.group(0).split()[1:-1]  # Drop SELECT and FROM
        if matched[0].strip() == 'DISTINCT':
            distinct = 'DISTINCT '
        else:
            distinct = ''
        data = defaultdict(list)
        order = []
        for i in matched:
            table, _, column = i.partition('.')
            column = column.rstrip(',')
            data[table].append(column)
            if table not in order and not table.startswith('DISTINCT'):
                order.append(table)
        new_cols = []
        for table in order:
            num = len(data[table])
            if num > 1:
                new_cols.append(f'{table}.*')
            elif num == 1:
                col = data[table][0]
                new_cols.append(f'{table}.{col}')
        repl = f'SELECT {distinct}' + ', '.join(new_cols) + ' FROM'
        stmt = re.sub(p, repl, stmt, count=1)
    return stmt


class PgStorage(SqlStorage):
    def __init__(self, dbname, url, session_id=None):
        super().__init__()
        self.placeholder = '%s'
        self.text_min = 'LEAST'
        self.text_max = 'GREATEST'
        self.ifnull = 'COALESCE'
        self.dbname = dbname
        self.infer_type = _infer_type
        self.defer_index = False
        if not session_id:
            session_id = 'firepit'
        self.session_id = session_id
        options = f'options=--search-path%3D{session_id}'
        sep = '&' if '?' in url else '?'
        connstring = f'{url}{sep}{options}'
        self.connection = psycopg2.connect(
            connstring,
            cursor_factory=psycopg2.extras.RealDictCursor)

        self._create_firepit_common_schema()
        if session_id:
            try:
                self._execute(f'CREATE SCHEMA IF NOT EXISTS "{session_id}";')
                # how to check if schema exists
            except psycopg2.errors.UniqueViolation:
                self.connection.rollback()

        self._execute(f'SET search_path TO "{session_id}", firepit_common;')

        stmt = ("SELECT (EXISTS (SELECT *"
                " FROM INFORMATION_SCHEMA.TABLES"
                " WHERE TABLE_SCHEMA = %s"
                " AND  TABLE_NAME = '__queries'))")
        res = self._query(stmt, (session_id,)).fetchone()
        done = list(res.values())[0] if res else False
        if not done:
            self._setup()
        else:
            self._checkdb()

        logger.debug("Connection to PostgreSQL DB %s successful", dbname)

    def _create_firepit_common_schema(self):
        try:
            stmt = ("SELECT routines.routine_name"
                    " FROM information_schema.routines"
                    " WHERE routines.specific_schema = 'firepit_common'")
            res = self._query(stmt).fetchall()
            if not res:
                self._execute('CREATE SCHEMA IF NOT EXISTS "firepit_common";')
                cursor = self._execute('BEGIN;')
                self._execute('''CREATE FUNCTION firepit_common.match(pattern TEXT, value TEXT)
                                RETURNS boolean AS $$
                                    SELECT regexp_match(value, pattern) IS NOT NULL;
                            $$ LANGUAGE SQL;''', cursor=cursor)
                self._execute(MATCH_BIN, cursor=cursor)
                self._execute(LIKE_BIN, cursor=cursor)
                self._execute('''CREATE FUNCTION firepit_common.in_subnet(addr TEXT, net TEXT)
                                RETURNS boolean AS $$
                                    SELECT addr::inet <<= net::inet;
                            $$ LANGUAGE SQL;''', cursor=cursor)
                cursor.close()
            elif len(res) < 4:
                # Might need to add new functions
                cursor = self._execute('BEGIN;')
                funcs = [r['routine_name'] for r in res]
                if 'match_bin' not in funcs:
                    self._execute(MATCH_BIN, cursor=cursor)
                if 'like_bin' not in funcs:
                    self._execute(LIKE_BIN, cursor=cursor)
                cursor.close()
        except psycopg2.errors.DuplicateFunction:
            self.connection.rollback()

    def _setup(self):
        cursor = self._execute('BEGIN;')
        try:
            # Do DB initization from base class
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__metadata" '
                    '(name TEXT, value TEXT);')
            self._execute(stmt, cursor)
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__symtable" '
                    '(name TEXT, type TEXT, appdata TEXT);')
            self._execute(stmt, cursor)
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__queries" '
                    '(sco_id TEXT, query_id TEXT);')
            self._execute(stmt, cursor)
            stmt = ('CREATE UNLOGGED TABLE IF NOT EXISTS "__contains" '
                    '(source_ref TEXT, target_ref TEXT, x_firepit_rank INTEGER,'
                    ' UNIQUE(source_ref, target_ref));')
            self._execute(stmt, cursor)
            self._set_meta(cursor, 'dbversion', DB_VERSION)
            self.connection.commit()
            cursor.close()
        except (psycopg2.errors.DuplicateFunction, psycopg2.errors.UniqueViolation):
            # We probably already created all these, so ignore this
            self.connection.rollback()

    def _get_writer(self, **kwargs):
        """Get a DB inserter object"""
        self.defer_index = kwargs.get('defer_index', self.defer_index)
        filedir = os.path.dirname(self.dbname)
        return SqlWriter(
            filedir,
            self,
            placeholder=self.placeholder,
            infer_type=_infer_type,
            **kwargs
        )

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
        except Exception as e:
            self.connection.rollback()
            logger.error('%s: %s', query, e, exc_info=e)
            raise UnexpectedError(str(e)) from e
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
            if not self.defer_index:
                self._create_index(tablename, cursor)
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

        # update all relevant viewdefs
        stmt = 'SELECT name, type FROM __symtable'
        cursor = self._query(stmt, (tablename,))
        rows = cursor.fetchall()
        for row in rows:
            logger.debug('%s', row)
        stmt = 'SELECT name FROM __symtable WHERE type = %s'
        cursor = self._query(stmt, (tablename,))
        rows = cursor.fetchall()
        for row in rows:
            viewname = row['name']
            viewdef = self._get_view_def(viewname)
            self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {viewdef}', cursor)

    def _create_empty_view(self, viewname, cursor):
        cursor.execute(f'CREATE VIEW "{viewname}" AS SELECT NULL as id WHERE 1<>1;')

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
            if not self._is_sql_view(viewname, cursor):
                # Must be a table...
                self._execute(f'ALTER TABLE "{viewname}" RENAME TO "_{viewname}"', cursor)
                slct = slct.replace(viewname, f'_{viewname}')
            # Swap out the viewname for its definition
            select = re.sub(f'FROM "{viewname}"', f'FROM ({slct}) AS tmp', select, count=1)
            select = re.sub(f'"{viewname}"', 'tmp', select)
        try:
            self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {select}', cursor)
        except psycopg2.errors.UndefinedTable as e:
            # Missing dep?
            self.connection.rollback()
            logger.error(e, exc_info=e)
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
        except psycopg2.errors.SyntaxError as e:
            # We see this on SQL injection attempts
            raise UnexpectedError(e.args[0]) from e
        except psycopg2.errors.UndefinedColumn as e:
            m = re.search(r'^column (.*) does not exist', e.args[0])
            raise InvalidAttr(m.group(1)) from e
        if is_new:
            self._new_name(cursor, viewname, sco_type)
        return cursor

    def _recreate_view(self, viewname, viewdef, cursor):
        self._execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {viewdef}', cursor)

    def _get_view_def(self, viewname):
        cursor = self._query("SELECT definition"
                             " FROM pg_views"
                             " WHERE schemaname = %s"
                             " AND viewname = %s", (self.session_id, viewname))
        viewdef = cursor.fetchone()
        if viewdef:
            stmt = viewdef['definition'].rstrip(';').replace('\n', ' ')

            # PostgreSQL will "expand" the original "*" to the columns
            # that existed at that time.  We need to get the star back, to
            # match SQLite3's behavior.
            logger.debug('%s original:  %s', viewname, stmt)
            stmt = _rewrite_query(stmt)
            logger.debug('%s rewritten: %s', viewname, stmt)
            return stmt

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

    def types(self, private=False):
        stmt = ("SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = %s AND table_type != 'VIEW'"
                "  EXCEPT SELECT name as table_name FROM __symtable")
        cursor = self._query(stmt, (self.session_id, ))
        rows = cursor.fetchall()
        if private:
            return [i['table_name'] for i in rows]
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

    def upsert_many(self, cursor, tablename, objs, query_id, schema, **kwargs):
        use_copy = kwargs.get('use_copy')
        if use_copy:
            self.upsert_copy(cursor, tablename, objs, query_id, schema)
        else:
            self.upsert_multirow(cursor, tablename, objs, query_id, schema)

    def upsert_multirow(self, cursor, tablename, objs, query_id, schema):
        colnames = list(schema.keys())
        quoted_colnames = [f'"{x}"' for x in colnames]
        valnames = ', '.join(quoted_colnames)

        placeholders = ', '.join([f"({', '.join([self.placeholder] * len(colnames))})"] * len(objs))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES {placeholders}'
        idx = None
        if 'id' in colnames:
            idx = colnames.index('id')
            action = 'NOTHING'
            if tablename != 'identity':
                excluded = self._get_excluded(colnames, tablename)
                if excluded:
                    action = f'UPDATE SET {excluded}'
            stmt += f' ON CONFLICT (id) DO {action}'
        elif tablename == '__contains':
            stmt += ' ON CONFLICT DO NOTHING'
        values = []
        query_values = []
        for obj in objs:
            if query_id and idx is not None:
                query_values.append(obj[idx])
                query_values.append(query_id)
            values.extend([ujson.dumps(value, ensure_ascii=False)
                           if isinstance(value, list) else value for value in obj])
        cursor.execute(stmt, values)

        if query_id and 'id' in colnames:
            # Now add to query table as well
            placeholders = ', '.join([f'({self.placeholder}, {self.placeholder})'] * len(objs))
            stmt = (f'INSERT INTO "__queries" (sco_id, query_id)'
                    f' VALUES {placeholders}')
            cursor.execute(stmt, query_values)

    def upsert_copy(self, cursor, tablename, objs, query_id, schema):
        colnames = list(schema.keys())
        quoted_colnames = [f'"{x}"' for x in colnames]
        valnames = ', '.join(quoted_colnames)

        # Create a temp table that copies the structure of `tablename`
        cursor.execute(f'CREATE TEMP TABLE tmp AS SELECT * FROM "{tablename}" WHERE 1=2;')

        # Create a generator over `objs` that returns text formatted objects
        copy_stmt = f"COPY tmp({valnames}) FROM STDIN WITH DELIMITER '{SEP}'"
        cursor.copy_expert(copy_stmt, ListToTextIO(objs, colnames, sep=SEP))

        # Now SELECT from TEMP table to real table
        stmt = (f'INSERT INTO "{tablename}" ({valnames})'
                f' SELECT {valnames} FROM tmp')
        if 'id' in colnames:
            action = 'NOTHING'
            if tablename != 'identity':
                excluded = self._get_excluded(colnames, tablename)
                if excluded:
                    action = f'UPDATE SET {excluded}'
            stmt += f'  ON CONFLICT (id) DO {action}'
        elif tablename == '__contains':
            stmt += ' ON CONFLICT DO NOTHING'
        cursor.execute(stmt)

        # Don't need the temp table anymore
        cursor.execute('DROP TABLE tmp')

        if query_id and 'id' in colnames:
            # Now add to query table as well
            idx = colnames.index('id')
            copy_stmt = f"COPY __queries(sco_id, query_id) FROM STDIN WITH DELIMITER '{SEP}'"
            qobjs = [(obj[idx], query_id) for obj in objs]
            cursor.copy_expert(copy_stmt, TuplesToTextIO(qobjs, ['sco_id', 'query_id'], sep=SEP))

    def finish(self, index=True):
        if index:
            cursor = self._query("SELECT table_name"
                                 " FROM information_schema.tables"
                                 " WHERE table_schema = %s"
                                 "   AND table_name IN (%s, %s)", (self.session_id, '__contains', '__reflist'))
            rows = cursor.fetchall()
            tables = [i['table_name'] for i in rows]
            cursor = self._execute('BEGIN;')
            if 'relationship' in self.tables():
                tables.append('relationship')
            for tablename in tables:
                self._create_index(tablename, cursor)
            self.connection.commit()
            cursor.close()
