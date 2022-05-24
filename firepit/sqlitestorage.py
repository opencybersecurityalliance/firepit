import ipaddress
import logging
import os
import re
import sqlite3

from base64 import b64decode

from firepit.exceptions import DuplicateTable
from firepit.exceptions import InvalidAttr
from firepit.exceptions import UnexpectedError
from firepit.exceptions import UnknownViewname
from firepit.splitter import SqlWriter
from firepit.sqlstorage import SqlStorage
from firepit.sqlstorage import infer_type
from firepit.sqlstorage import validate_name

logger = logging.getLogger(__name__)


def get_storage(path):
    return SQLiteStorage(path)


def _in_subnet(value, net):
    """User-defined function to help implement STIX ISSUBSET"""
    if '/' in value:
        value = ipaddress.IPv4Network(value).network_address
    else:
        value = ipaddress.IPv4Address(value)
    net = ipaddress.IPv4Network(net)
    return value in net


def _match(pattern, value):
    """User-defined function to implement SQL MATCH/STIX MATCHES"""
    return (value is not None and
            bool(re.search(pattern, value, re.DOTALL)))


def _match_bin(pattern, value):
    """User-defined function to implement SQL MATCH/STIX MATCHES for binaries"""
    if value is not None:
        val = b64decode(value).decode("utf-8")
        return bool(re.search(pattern, val, re.DOTALL))
    return False

def _like_bin(pattern, value):
    """User-defined function to implement SQL/STIX LIKE for binaries"""
    if value is not None:
        try:
            exp = re.escape(pattern).replace('%', '.*').replace('_', '.')
            val = b64decode(value).decode("utf-8")
            return bool(re.search(exp, val, re.DOTALL))
        except Exception as e:
            logger.error('%s', e, exc_info=e)
    return False


class SQLiteStorage(SqlStorage):
    def __init__(self, dbname):
        super().__init__()
        self.placeholder = '?'
        self.dbname = dbname
        self.connection = sqlite3.connect(dbname)
        self.connection.row_factory = row_factory
        logger.debug("Connection to SQLite DB %s successful", dbname)

        # Create functions for IP address subnet membership checks
        self.connection.create_function('in_subnet', 2, _in_subnet)

        # Create function for SQL MATCH
        self.connection.create_function("match", 2, _match)
        self.connection.create_function("match_bin", 2, _match_bin)
        self.connection.create_function("like_bin", 2, _like_bin)

        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='__queries'")
        rows = cursor.fetchall()
        if len(rows) == 1:
            # Attaching to existing DB
            self._checkdb()
        else:
            # Do DB initization
            cursor.execute('BEGIN;')
            self._initdb(cursor)
        cursor.close()

    def _get_writer(self, **kwargs):
        """Get a DB inserter object"""
        filedir = os.path.dirname(self.dbname)
        return SqlWriter(filedir, self, infer_type=infer_type)

    def _do_execute(self, query, values=None, cursor=None):
        if not cursor:
            cursor = self.connection.cursor()
        try:
            logger.debug('Executing query: %s', query)
            if not values:
                cursor.execute(query)
            else:
                cursor.execute(query, values)
        except sqlite3.OperationalError as e:
            logger.error('%s: %s', query, e)  #, exc_info=e)
            if e.args[0].startswith("no such column"):
                m = e.args[0].replace("no such column", "invalid attribute")
                raise InvalidAttr(m) from e
            elif e.args[0].startswith("no such table: main."):
                # Just means no match - return empty cursor?
                cursor = self.connection.cursor()
            elif e.args[0].startswith("no such table: "):
                raise UnknownViewname(e.args[0]) from e
            elif e.args[0].endswith("syntax error"):
                # We see this on SQL injection attempts
                raise UnexpectedError(e.args[0]) from e
            else:
                raise e  # See if caller wants special behavior
        return cursor

    def _execute(self, statement, cursor=None):
        return self._do_execute(statement, cursor=cursor)

    def _query(self, query, values=None, cursor=None):
        cursor = self._do_execute(query, values=values, cursor=cursor)
        self.connection.commit()
        return cursor

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
            select = re.sub(f'FROM "{viewname}"', f'FROM ({slct}) AS tmp', select, count=1)
            select = re.sub(f'"{viewname}"', 'tmp', select)
        if self._is_sql_view(viewname, cursor):
            is_new = False
            self._execute(f'DROP VIEW IF EXISTS "{viewname}"', cursor)
        self._execute(f'CREATE VIEW "{viewname}" AS {select}', cursor)
        if is_new:
            self._new_name(cursor, viewname, sco_type)
        return cursor

    def _create_table(self, tablename, columns):
        stmt = f'CREATE TABLE "{tablename}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in columns.items()])
        stmt += ');'
        logger.debug('_create_table: "%s"', stmt)
        try:
            cursor = self._execute(stmt)
        except sqlite3.OperationalError as e:
            self.connection.rollback()
            logger.debug('_create_table: %s', e)  #, exc_info=e)
            if e.args[0].startswith(f'table "{tablename}" already exists'):
                raise DuplicateTable(tablename) from e
        self._create_index(tablename, cursor)
        self.connection.commit()
        cursor.close()

    def _add_column(self, tablename, prop_name, prop_type):
        stmt = f'ALTER TABLE "{tablename}" ADD COLUMN "{prop_name}" {prop_type};'
        logger.debug('new_property: "%s"', stmt)
        try:
            cursor = self._execute(stmt)
            self.connection.commit()
            cursor.close()
        except sqlite3.OperationalError as e:
            self.connection.rollback()
            logger.debug('%s', e)  #, exc_info=e)
            if e.args[0].startswith('duplicate column name: '):
                pass
            else:
                raise Exception('Internal error: ' + e.args[0]) from e

    def _get_view_def(self, viewname):
        view = self._query(("SELECT sql from sqlite_master"
                            " WHERE type='view' and name=?"),
                           values=(viewname,)).fetchone()
        if view:
            slct = view['sql']
            return slct.replace(f'CREATE VIEW "{viewname}" AS ', '')

        # Must be a table
        return f'SELECT * FROM "{viewname}"'

    def _is_sql_view(self, name, cursor=None):
        view = self._query(("SELECT sql from sqlite_master"
                            " WHERE type='view' and name=?"),
                           values=(name,)).fetchone()
        return view is not None

    def tables(self):
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table';")
        rows = cursor.fetchall()
        return [i['name'] for i in rows
                if not i['name'].startswith('__') and
                not i['name'].startswith('sqlite')]

    def types(self, private=False):
        stmt = ("SELECT name FROM sqlite_master WHERE type='table'"
                " EXCEPT SELECT name FROM __symtable")
        cursor = self.connection.execute(stmt)
        rows = cursor.fetchall()
        if private:
            return [i['name'] for i in rows]
        return [i['name'] for i in rows
                if not i['name'].startswith('__') and
                not i['name'].startswith('sqlite')]

    def columns(self, viewname):
        validate_name(viewname)
        stmt = f'PRAGMA table_info("{viewname}")'
        cursor = self._execute(stmt)
        try:
            mappings = cursor.fetchall()
            if mappings:
                result = [e["name"] for e in mappings]
            else:
                result = []
            logger.debug('%s columns = %s', viewname, result)
        except sqlite3.OperationalError as e:
            logger.error('%s', e)
            result = []
        return result

    def schema(self, viewname):
        validate_name(viewname)
        stmt = f'PRAGMA table_info("{viewname}")'
        cursor = self._execute(stmt)
        return [{k: v for k, v in row.items() if k in ['name', 'type']}
                for row in cursor.fetchall()]

    def delete(self):
        """Delete ALL data in this store"""
        self.connection.close()
        try:
            os.remove(self.dbname)
        except FileNotFoundError:
            pass


def row_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
