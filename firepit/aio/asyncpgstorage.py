import logging
import re
from collections import OrderedDict, defaultdict
from random import randrange

import asyncpg
import pandas as pd

import ujson
from firepit.aio.asyncstorage import AsyncStorage
from firepit.deref import auto_deref_cached
from firepit.exceptions import (InvalidAttr, InvalidStixPath, UnknownViewname,
                                SessionExists, SessionNotFound, DuplicateTable)
from firepit.pgcommon import (CHECK_FOR_COMMON_SCHEMA,
                              CHECK_FOR_QUERIES_TABLE, INTERNAL_TABLES,
                              LIKE_BIN, MATCH_BIN, MATCH_FUN, SUBNET_FUN,
                              _rewrite_view_def, _infer_type, pg_shorten)
from firepit.query import Column, Limit, Offset, Order, Projection, Query
from firepit.splitter import RecordList
from firepit.sqlstorage import (DB_VERSION, _format_query,
                                _make_aggs, _transform)
from firepit.validate import validate_name, validate_path

logger = logging.getLogger(__name__)


def get_storage(connstring, session_id):
    return AsyncpgStorage(connstring, session_id)


def get_placeholders(n):
    return [f'${i}' for i in range(1, n + 1)]


class AsyncpgStorage(AsyncStorage):
    class Placeholder:
        def __init__(self, offset=0):
            self.i = offset + 1

        def __str__(self):
            s = str(self.i)
            self.i += 1
            return f'${s}'

    def __init__(self, connstring: str, session_id: str):
        validate_name(session_id)
        self.placeholder = '%s'
        self.dialect = 'postgresql'
        self.connstring = connstring
        self.session_id = session_id
        self.conn = None

        # SQL data type inference function (database-specific)
        self.infer_type = _infer_type

        # SQL column name function (database-specific)
        self.shorten = pg_shorten

    async def create(self, ssl_context=None):
        """
        Create a new "session" (PostgreSQL schema).  Fail if it already exists.
        """
        logger.debug('Creating storage for session %s', self.session_id)
        self.conn = await asyncpg.connect(self.connstring, ssl=ssl_context)

        # common schema
        async with self.conn.transaction():
            res = await self.conn.fetch(CHECK_FOR_COMMON_SCHEMA)
            if not res:
                await self.conn.execute('CREATE SCHEMA IF NOT EXISTS "firepit_common";')
                await self.conn.execute(MATCH_FUN)
                await self.conn.execute(MATCH_BIN)
                await self.conn.execute(LIKE_BIN)
                await self.conn.execute(SUBNET_FUN)

        # fail if it already exists
        try:
            await self.conn.execute(f'CREATE SCHEMA "{self.session_id}"')
        except asyncpg.exceptions.DuplicateSchemaError:
            raise SessionExists(self.session_id)
        await self.conn.execute(f'SET search_path TO "{self.session_id}", firepit_common')

        async with self.conn.transaction():
            # create tables, etc.
            for stmt in INTERNAL_TABLES:
                logger.debug('%s', stmt)
                await self.conn.execute(stmt)
            # Record db version
            await self._set_meta('dbversion', DB_VERSION)

    async def attach(self):
        """
        Attach/connect to an existing session.  Fail if it doesn't exist.
        """
        logger.debug('Attaching to storage for session %s', self.session_id)
        self.conn = await asyncpg.connect(self.connstring)

        # fail if it doesn't exist
        result = await self.conn.fetch(
            ("SELECT schema_name"
             " FROM information_schema.schemata"
             f" WHERE schema_name = '{self.session_id}'"))
        if not result:
            raise SessionNotFound(self.session_id)

        await self.conn.execute(f'SET search_path TO "{self.session_id}", firepit_common')

    async def cache(self,
                    query_id: str,
                    bundle: dict):
        """
        Ingest a single, in-memory STIX bundle, labelled with `query_id`.
        """
        splitter = AsyncSplitWriter(self, query_id=str(query_id))
        await splitter.init()

        for obj in _transform(bundle):
            await splitter.write(obj)
        await splitter.close()

    async def tables(self):
        rows = await self.conn.fetch(
            ("SELECT table_name"
             " FROM information_schema.tables"
             " WHERE table_schema = $1"
             "   AND table_type != 'VIEW'"),
            self.session_id
        )
        return [i['table_name'] for i in rows
                if not i['table_name'].startswith('__')]

    async def views(self):
        """Get all view names"""
        result = await self.conn.fetch('SELECT name FROM __symtable')
        return [row['name'] for row in result]

    async def table_type(self, viewname):
        """Get the SCO type for table/view `viewname`"""
        validate_name(viewname)
        stmt = 'SELECT "type" FROM "__symtable" WHERE name = $1'
        row = await self.conn.fetchrow(stmt, viewname)
        return row['type'] if row else None

    async def types(self, private=False):
        stmt = ("SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = $1 AND table_type != 'VIEW'"
                "  EXCEPT SELECT name as table_name FROM __symtable")
        rows = await self.conn.fetch(stmt, self.session_id)
        if private:
            tables = [i['table_name'] for i in rows]
        else:
            # Ignore names that start with 1 or 2 underscores
            tables = [i['table_name'] for i in rows
                      if not i['table_name'].startswith('_')]
        return tables

    async def columns(self, viewname):
        """Get the column names (properties) of `viewname`"""
        validate_name(viewname)
        stmt = ("SELECT column_name"
                " FROM information_schema.columns"
                " WHERE table_schema = $1"
                " AND table_name = $2")
        rows = await self.conn.fetch(stmt, self.session_id, viewname)
        return [i['column_name'] for i in rows]

    async def schema(self, viewname=None):
        """Get the schema (names and types) of `viewname`"""
        if viewname:
            validate_name(viewname)
            stmt = ("SELECT column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = $1"
                    " AND table_name = $2")
            rows = await self.conn.fetch(stmt, self.session_id, viewname)
        else:
            stmt = ("SELECT table_name AS name, column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = $1")
            rows = await self.conn.fetch(stmt, self.session_id)
        return [dict(row) for row in rows]

    async def delete(self):
        """Delete ALL data in this store"""
        try:
            stmt = (f'DROP SCHEMA "{self.session_id}" CASCADE')
            await self.conn.execute(stmt)
        except asyncpg.exceptions.InvalidSchemaNameError as e:
            raise SessionNotFound(self.session_id) from e

    async def set_appdata(self, viewname, data):
        """Attach app-specific data to a viewname"""
        validate_name(viewname)
        stmt = ('UPDATE "__symtable" SET appdata = $1'
                ' WHERE name = $2')
        await self.conn.execute(stmt, data, viewname)

    async def get_appdata(self, viewname):
        """Retrieve app-specific data for a viewname"""
        validate_name(viewname)
        stmt = 'SELECT appdata FROM "__symtable" WHERE name = $1'
        res = await self.conn.fetchrow(stmt, viewname)
        if not res:
            return None
        if 'appdata' in res:
            return res['appdata']
        return dict(res[0])

    async def get_view_data(self, viewnames=None):
        """Retrieve information about one or more viewnames"""
        if viewnames:
            placeholders = ', '.join(get_placeholders(len(viewnames)))
            stmt = f'SELECT * FROM "__symtable" WHERE name IN ({placeholders});'
            rows = await self.conn.fetch(stmt, *viewnames)
        else:
            stmt = 'SELECT * FROM "__symtable";'
            rows = await self.conn.fetch(stmt)
        return [dict(row) for row in rows]

    async def run_query(self, query: Query):
        query_text, query_values = query.render(self.placeholder, self.dialect)
        result = await self.fetch(query_text, *query_values)
        return [dict(r) for r in result]

    async def fetch(self, query, *args):
        """Passthrough to underlying DB"""
        if '%s' in query:
            n = len(args) if args is not None else 0
            query = query % tuple(f'${i}' for i in range(1, n + 1))
        try:
            result = await self.conn.fetch(query, *args)
        except asyncpg.exceptions.UndefinedColumnError as e:
            raise InvalidAttr(str(e)) from e
        except asyncpg.exceptions.UndefinedTableError as e:
            raise UnknownViewname(str(e)) from e
        return result

    async def fetchrow(self, query, *args):
        """Passthrough to underlying DB"""
        result = await self.conn.fetchrow(query, *args)
        return result

    async def remove_view(self, viewname):
        """Remove view `viewname`"""
        validate_name(viewname)
        async with self.conn.transaction():
            await self.conn.execute(f'DROP VIEW IF EXISTS "{viewname}"')
            await self._drop_name(viewname)

    async def assign_query(self, viewname, query, sco_type=None):
        """
        Create a new view `viewname` defined by `query`
        """
        # Deduce SCO type and "deps" of viewname from query
        on = query.table.name
        deps = [on]
        schema = None
        if not sco_type:
            sco_type = await self.table_type(on)
            logger.debug('Deduced type of %s as %s', viewname, sco_type)
        if query.groupby:
            if not bool(query.aggs) and sco_type:
                schema = await self.schema(sco_type)

                # if no aggs supplied, do "auto aggregation"
                if schema:
                    query.aggs = _make_aggs(query.groupby.cols, sco_type, schema)

        stmt = _format_query(query, self.dialect)
        logger.debug('assign_query: %s', stmt)
        await self._create_view(viewname, stmt, sco_type, deps=deps)

    async def lookup(self, viewname, cols="*", limit=None, offset=None, col_dict=None):
        """Get the value of `viewname`"""
        # Preserve sort order, if it's been specified
        # The joins below can reorder
        viewdef = await self._get_view_def(viewname)
        match = re.search(r"ORDER BY \"([a-z0-9:'\._\-]*)\" (ASC|DESC)$", viewdef)
        if match:
            sort = (Column(match.group(1), viewname), match.group(2))
        else:
            sort = None
        qry = Query(viewname)
        if cols != "*":
            dbcols = await self.columns(viewname)
            if isinstance(cols, str):
                cols = cols.replace(" ", "").split(",")
            proj = []
            for col in cols:
                if col not in dbcols:
                    try:
                        validate_path(col)
                    except InvalidStixPath as e:
                        raise InvalidAttr(f"{col}") from e
                    joins, target_table, target_column = await self.path_joins(viewname, None, col)
                    qry.extend(joins)
                    proj.append(Column(target_column, table=target_table, alias=col))
                else:
                    proj.append(Column(col, viewname))
            qry.append(Projection(proj))
        else:
            if not col_dict:
                col_dict = await self._get_col_dict()
            dbcols = await self.columns(viewname)
            joins, proj = auto_deref_cached(viewname, dbcols, col_dict)
            if joins:
                qry.extend(joins)
            if proj:
                qry.append(proj)
        if sort:
            qry.append(Order([sort]))
        if limit:
            qry.append(Limit(limit))
        if offset:
            qry.append(Offset(offset))
        results = await self.run_query(qry)
        if 'type' in cols or cols == '*':
            sco_type = await self.table_type(viewname)
            if not sco_type:
                sco_type = viewname
            for result in results:
                result['type'] = sco_type
        return results

    # "Private" API
    async def _get_view_def(self, viewname):
        stmt = ("SELECT definition"
                " FROM pg_views"
                " WHERE schemaname = $1"
                " AND viewname = $2")
        viewdef = await self.conn.fetchrow(stmt, self.session_id, viewname)
        return _rewrite_view_def(viewname, viewdef)

    async def _set_meta(self, name, value):
        stmt = ('INSERT INTO "__metadata" (name, value) VALUES ($1, $2)')
        await self.conn.execute(stmt, name, value)

    async def _new_name(self, name, sco_type):
        stmt = ('INSERT INTO "__symtable" (name, type) VALUES ($1, $2)'
                ' ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type')
        await self.conn.execute(stmt, name, sco_type)

    async def _drop_name(self, name):
        stmt = 'DELETE FROM "__symtable" WHERE name = $1'
        await self.conn.execute(stmt, name)

    async def _create_view(self, viewname, select, sco_type, deps=None):
        """Overrides parent"""
        validate_name(viewname)
        is_new = True
        async with self.conn.transaction():
            if not deps:
                deps = []
            elif viewname in deps:
                is_new = False
                # Get the query that makes up the current view
                slct = await self._get_view_def(viewname)
                if not self._is_sql_view(viewname):
                    # Must be a table...
                    await self.conn.execute(f'ALTER TABLE "{viewname}" RENAME TO "_{viewname}"')
                    slct = slct.replace(viewname, f'_{viewname}')
                # Swap out the viewname for its definition
                select = re.sub(f'FROM "{viewname}"', f'FROM ({slct}) AS tmp', select, count=1)
                select = re.sub(f'"{viewname}"', 'tmp', select)
            await self.conn.execute(f'CREATE OR REPLACE VIEW "{viewname}" AS {select}')
            if is_new:
                await self._new_name(viewname, sco_type)

    async def _is_sql_view(self, name):
        viewdef = await self.conn.fetchrow(
            "SELECT definition"
            " FROM pg_views"
            " WHERE schemaname = $1"
            " AND viewname = $2",
            self.session_id, name)
        return viewdef is not None

    async def _get_col_dict(self):
        q = Query('__columns')
        col_dict = defaultdict(list)
        results = await self.run_query(q)
        for result in results:
            col_dict[result['otype']].append(result['path'])
        return col_dict

    # The former AsyncSqlWriter interface
    async def _replace(self, cursor, tablename, obj, schema):
        colnames = schema.keys()
        valnames = ', '.join([f'"{x}"' for x in colnames])
        placeholders = ', '.join(get_placeholders(len(obj)))
        stmt = f'INSERT INTO "{tablename}" ({valnames}) VALUES ({placeholders})'
        if 'id' in colnames:
            stmt += ' ON CONFLICT (id) DO '
            valnames = [f'"{col}" = EXCLUDED."{col}"' for col in colnames if col != 'id']
            valnames = ', '.join(valnames)
            stmt += f'UPDATE SET {valnames};'
        tmp = [ujson.dumps(value, ensure_ascii=False)
               if isinstance(value, list) else value for value in obj]
        values = tuple(tmp)
        logger.debug('_replace: "%s" values %s', stmt, values)
        await cursor.execute(stmt, values)

    async def new_type(self, obj_type, schema):
        # Same as base class, but disable WAL
        stmt = f'CREATE UNLOGGED TABLE "{obj_type}" ('
        stmt += ','.join([f'"{colname}" {coltype}' for colname, coltype in schema.items()])
        stmt += ')'
        logger.debug('new_table: %s', stmt)
        try:
            await self.conn.execute(stmt)
        except asyncpg.exceptions.DuplicateTableError as e:
            raise DuplicateTable(obj_type) from e

    async def new_property(self, obj_type, prop_name, prop_type):
        stmt = f'ALTER TABLE "{obj_type}" ADD COLUMN "{prop_name}" {prop_type}'
        logger.debug('new_property: %s', stmt)
        await self.conn.execute(stmt)

    async def write_df(self, tablename, df, query_id, schema):
        # Generate random tmp table name
        r = randrange(0, 1000000)
        tmp = f'tmp{r}_{tablename}'
        columns = df.columns
        for col in columns:
            if col not in schema:
                #df = df.drop(columns=col)
                continue
            stype = schema[col].lower()
            if stype == 'text':
                df[col] = df[col].astype('string')
            elif stype == 'numeric':
                df[col] = df[col].astype('UInt64')
            elif stype == 'bigint':
                df[col] = df[col].astype('Int64')
            elif stype == 'integer':
                df[col] = df[col].astype('Int32')
            elif stype == 'boolean':
                df[col] = df[col].astype('boolean')

        #TODO: if no id column, use drop_duplicates?

        # Not sure how it could have survived, but it did for "__columns"  FIXME
        if 'type' in df.columns:
            df = df.drop(columns='type')

        colnames = list(df.columns)  #schema.keys())
        quoted_colnames = [f'"{x}"' for x in colnames]
        valnames = ', '.join(quoted_colnames)

        # Replace NaNs with None, since asyncpg won't do it
        # Then convert back to "record" format
        try:
            # No need to reorder if we create tmp table off our columns only
            records = df.replace({pd.NA: None}).to_records(index=False)
        except KeyError as e:
            logger.error('df.columns = %s', df.columns)
            logger.error('%s', e, exc_info=e)
            raise e

        async with self.conn.transaction():
            # Create a temp table first
            s = ', '.join([f'"{name}" {schema[name]}' for name in colnames])
            stmt = f'CREATE TEMP TABLE "{tmp}" ({s})'
            logger.debug('%s', stmt)
            await self.conn.execute(stmt)

            # Copy the records into the temp table
            try:
                await self.conn.copy_records_to_table(tmp, records=records)
            except asyncpg.exceptions.BadCopyFileFormatError as e:
                # Log and re-raise
                logger.critical('%s', e, exc_info=e)
                raise e

            # Now SELECT from temp table to real table
            stmt = (f'INSERT INTO "{tablename}" ({valnames})'
                    f' SELECT {valnames} FROM "{tmp}"')
            if 'id' in colnames:
                stmt += ' ORDER BY id'  # Avoid deadlocks
                action = 'NOTHING'
                if tablename != 'identity':
                    excluded = _get_excluded(colnames, tablename)
                    if excluded:
                        action = f'UPDATE SET {excluded}'
                stmt += f'  ON CONFLICT (id) DO {action}'
            else:
                stmt += f' ORDER BY "{colnames[0]}"'  # Any port in a storm...
                stmt += ' ON CONFLICT DO NOTHING'
            logger.debug('upsert: %s', stmt)
            try:
                await self.conn.execute(stmt)
            except asyncpg.exceptions.CardinalityViolationError as e:
                logger.error('CardinalityViolationError for %s', tablename)
                logger.critical('%s', e, exc_info=e)
                raise e

            # Don't need the temp table anymore
            await self.conn.execute(f'DROP TABLE "{tmp}"')

        if query_id and 'id' in colnames:
            # Now add to query table as well
            idx = colnames.index('id')
            qobjs = [(obj[idx], query_id) for obj in records]
            await self.conn.copy_records_to_table('__queries', records=qobjs)

    async def properties(self, obj_type=None):
        if obj_type:
            stmt = ("SELECT column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = $1"
                    " AND table_name = $2")
            rows = await self.fetch(stmt, self.session_id, obj_type)
            logger.debug('Schema "%s" rows: %s', obj_type, rows)
        else:
            stmt = ("SELECT table_name AS table, column_name AS name, data_type AS type"
                    " FROM information_schema.columns"
                    " WHERE table_schema = $1")
            rows = await self.fetch(stmt, self.session_id)
        return [dict(row) for row in rows]


class AsyncRecordList(RecordList):
    def __init__(self):
        super().__init__(1)

    def append(self, record):
        rec_id = record.get('id', len(self.records))
        if rec_id in self.records:
            # Update record instead
            self.records[rec_id].update(record)
        else:
            self.records[rec_id] = record


class AsyncSplitWriter:
    """
    Writes STIX objects using `writer`.  This class will track schema
    changes and store `batchsize` objects in memory before passing to
    `writer`.

    """

    def __init__(self, writer, extras=None, replace=False, query_id=None):
        self.schemas = {}
        self.writer = writer
        self.records = {}
        self.extras = extras or {}
        self.replace = replace
        self.query_id = query_id

    async def init(self):
        session_id = self.writer.session_id
        conn = self.writer.conn
        try:
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{session_id}"')
        except asyncpg.exceptions.UniqueViolationError:
            pass  # Potential race condition?
        await conn.execute(f'SET search_path TO "{session_id}", firepit_common')

        stmt = CHECK_FOR_QUERIES_TABLE.replace('%s', '$1')  # HACK
        row = await conn.fetch(stmt, session_id)
        if not row[0]:
            # Create all tables in a single transaction
            async with conn.transaction():
                for stmt in INTERNAL_TABLES:
                    await conn.execute(stmt)

        # Can't async load these in ctor, so do it here
        await self._load_schemas()

    def __del__(self):
        pass

    async def _load_schema(self, obj_type):
        props = await self.writer.properties(obj_type)
        schema = OrderedDict({col['name']: col['type'] for col in props})
        logger.debug('Loaded "%s" schema (%d columns total)', obj_type, len(schema))
        return schema

    async def _load_schemas(self):
        cols = await self.writer.properties()
        for col in cols:
            schema = self.schemas.get(col['table'], {})
            schema[col['name']] = col['type']
            self.schemas[col['table']] = schema
        logger.debug("Loaded %d schemas (%d columns total)", len(self.schemas), len(cols))

    async def write(self, obj):
        """Consume `obj` (actual writing to storage may be deferred)"""
        obj.update(self.extras)  # unused?
        obj_type = obj['type']
        schema = self.schemas.get(obj_type)
        add_table = False
        add_col = False
        new_columns = {}
        if obj_type == '__columns':
            pass
        else:
            new_obj = {}
            if not schema:
                # Found new table
                schema = OrderedDict()
                add_table = True
            #if obj_type == '__contains' and 'x_firepit_rank' not in obj:
            #    obj['x_firepit_rank'] = 1   # TEMP: HACK.
            for key, value in obj.items():
                if key in ['type', 'spec_version']:
                    continue
                # shorten key (STIX prop) to make column names more manageable
                if len(key) > 63 or 'extensions.' in key:
                    shortname = self.writer.shorten(key)  # Need to detect collisions!
                else:
                    shortname = key
                new_obj[shortname] = value
                if shortname not in schema:
                    # Found new column
                    if not add_table:
                        add_col = True
                    await self.write({'type': '__columns',
                                      'otype': obj_type,
                                      'path': key,
                                      'shortname': shortname,
                                      'dtype': value.__class__.__name__})
                    col_type = self.writer.infer_type(key, value)
                    schema[shortname] = col_type
                    new_columns[shortname] = col_type
            obj = new_obj
        if add_table:
            self.schemas[obj_type] = schema
            try:
                await self.writer.new_type(obj_type, schema)
                logger.debug('Added new type %s', obj_type)
            except (DuplicateTable,
                    asyncpg.exceptions.DuplicateTableError,
                    asyncpg.exceptions.UniqueViolationError,
                    asyncpg.exceptions.DuplicateObjectError):
                logger.debug('Failed to add %s; refreshing schemas', obj_type)
                # Refresh schemas
                loaded_schema = await self._load_schema(obj_type)
                new_columns = {}
                # We only need the new columns we discovered
                # Don't care if we loaded new ones
                for key in set(schema) - set(loaded_schema):
                    new_columns[key] = schema[key]
                    add_col = True
        if obj_type in self.records:
            reclist = self.records[obj_type]
        else:
            reclist = AsyncRecordList()
            self.records[obj_type] = reclist
        if add_col:
            for col, col_type in new_columns.items():
                # No need to add Nones if we're collecting dicts, then using DataFrame
                try:
                    await self.writer.new_property(obj_type, col, col_type)
                except (asyncpg.exceptions.DuplicateColumnError,
                        asyncpg.exceptions.DuplicateObjectError) as e:
                    logger.debug('%s', e)
        rec = {}
        for key, val in obj.items():
            if key == 'ipfix.flowId':  # F'in HACK CITY
                rec[key] = str(val)
            elif isinstance(val, list):
                rec[key] = ujson.dumps(val, ensure_ascii=False)
            else:
                rec[key] = val
        reclist.append(rec)

    async def close(self):
        logger.debug('close')
        for obj_type, recs in self.records.items():
            logger.debug('close %d %s', len(recs), obj_type)
            if recs:
                schema = self.schemas[obj_type]
                try:
                    await self.writer.write_records(obj_type, recs, schema, self.replace, self.query_id)
                except (TypeError, ValueError) as e:
                    logger.error('Exception while writing %s', obj_type)
                    logger.error('%s', e, exc_info=e)
                    raise e


# adapted from SqlStorage
def _get_excluded(colnames, tablename):
    text_min = 'LEAST'
    text_max = 'GREATEST'
    excluded = []
    for col in colnames:
        if col == 'first_observed':
            excluded.append(f'first_observed = {text_min}("{tablename}".first_observed, EXCLUDED.first_observed)')
        elif col == 'last_observed':
            excluded.append(f'last_observed = {text_max}("{tablename}".last_observed, EXCLUDED.last_observed)')
        elif col == 'number_observed':
            excluded.append(f'number_observed = "{tablename}".number_observed + EXCLUDED.number_observed')
        elif col == 'id':
            continue
        else:
            excluded.append(f'"{col}" = COALESCE(EXCLUDED."{col}", "{tablename}"."{col}")')
    return ', '.join(excluded)
