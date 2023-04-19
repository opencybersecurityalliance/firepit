"""
EXPERIMENTAL
async/await-based interface
"""

import logging

import pandas as pd

from firepit.props import parse_prop, prop_metadata
from firepit.query import Query
from firepit.splitter import shorten_extension_name
from firepit.sqlstorage import get_path_joins, infer_type


logger = logging.getLogger(__name__)


class AsyncStorage:
    """
    async/await-based local storage interface
    """
    def __init__(self, connstring: str, session_id: str):
        self.placeholder = '%s'
        self.dialect = None
        self.connstring = connstring
        self.session_id = session_id
        self.conn = None

        # SQL data type inference function (database-specific)
        self.infer_type = infer_type

        # SQL column name function (database-specific)
        self.shorten = shorten_extension_name

    async def create(self, ssl_context=None):
        """
        Create a new "session".  Should fail if it already exists.
        """
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.create')

    async def attach(self):
        """
        Attach/connect to an existing session.  Should fail if it doesn't exist.
        """
         # This is DB-specific
        raise NotImplementedError('AsyncStorage.attach')

    async def cache(self,
                    query_id: str,
                    bundle: dict):
        """
        Ingest a single, in-memory STIX bundle, labelled with `query_id`.
        """
         # This is DB-specific
        raise NotImplementedError('AsyncStorage.cache')

    async def tables(self):
         # This is DB-specific
        raise NotImplementedError('AsyncStorage.tables')

    async def views(self):
        """Get all view names"""
         # This is DB-specific
        raise NotImplementedError('AsyncStorage.views')

    async def table_type(self, viewname):
        """Get the SCO type for table/view `viewname`"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.table_type')

    async def types(self, private=False):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.types')

    async def columns(self, viewname):
        """Get the column names (properties) of `viewname`"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.columns')

    async def schema(self, viewname=None):
        """Get the schema (names and types) of `viewname`"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.schema')

    async def delete(self):
        """Delete ALL data in this session"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.delete')

    async def set_appdata(self, viewname, data):
        """Attach app-specific data to a viewname"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.set_appdata')

    async def get_appdata(self, viewname):
        """Retrieve app-specific data for a viewname"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.get_appdata')

    async def get_view_data(self, viewnames=None):
        """Retrieve information about one or more viewnames"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.get_viewdata')

    async def run_query(self, query: Query):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.run_query')

    async def fetch(self, query, *args):  #TODO: remove? make private?
        """Passthrough to underlying DB"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.fetch')

    async def fetchrow(self, query, *args):  #TODO: remove? make private?
        """Passthrough to underlying DB"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.fetchrow')

    async def query(self, query, values=None):
        logger.debug('Executing query: %s', query)
        if values:
            result = await self.fetch(query, *values)
        else:
            result = await self.fetch(query)
        return [dict(r) for r in result]

    async def remove_view(self, viewname):
        """Remove view `viewname`"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.remove_view')

    async def assign_query(self, viewname, query, sco_type=None):
        """
        Create a new view `viewname` defined by `query`
        """
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.assign_query')

    async def lookup(self, viewname, cols="*", limit=None, offset=None, col_dict=None):
        """Get the value of `viewname`"""
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.lookup')

    async def path_joins(self, viewname, sco_type, column):
        if not sco_type:
            sco_type = await self.table_type(viewname)
        return get_path_joins(viewname, sco_type, column)

    # "Private" API
    async def _is_sql_view(self, name):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage._is_sql_view')

    # The former AsyncSqlWriter interface
    async def new_type(self, obj_type, schema):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.new_type')

    async def new_property(self, obj_type, prop_name, prop_type):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.new_property')

    async def write_records(self, obj_type, records, schema, replace, query_id):
        logger.debug('Writing %d %s objects (%d props)', len(records), obj_type, len(schema))

        # Load records into dataframe and do type conversions as required
        df = pd.DataFrame(records)
        if 'type' in df.columns:
            # We don't need the type column since each table *is* a type
            df = df.drop('type', axis=1)
        await self.write_df(obj_type, df, query_id, schema)

    async def write_df(self, tablename, df, query_id, schema):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.write_df')

    async def properties(self, obj_type=None):
        # This is DB-specific
        raise NotImplementedError('AsyncStorage.properties')


class AsyncDBCache:
    def __init__(self, store: AsyncStorage):
        self.store = store
        self.table_set = {'observed-data', 'identity'}  # Should always be present
        self.type_set = set()
        self.view_set = set()
        self.col_dict = {}
        self.schema_dict = {}
        self.meta_dict = {}  # table -> column -> meta
        #self._get_metadata()

    async def _get_metadata(self):
        """For backwards compat"""
        return await self.get_metadata()

    async def get_metadata(self):
        q = Query('__columns')
        results = await self.store.run_query(q)
        for result in results:
            otype = result['otype']
            # Create entry for this table if necessary
            if otype not in self.meta_dict:
                self.meta_dict[otype] = {}
            metadict = self.meta_dict[otype]
            # Create entry for this column
            metadict[result['shortname']] = result

        # fill in self.col_dict and friends
        for table, data in self.meta_dict.items():
            self.table_set.add(table)
            self.col_dict[table] = sorted(data.keys())

        logger.debug('DBCache: Preload columns for "observed-data"')
        for table in ('observed-data', 'identity'):  # Hacky
            cols = set(await self.store.columns(table))
            if table in self.col_dict:
                old_cols = set(self.col_dict[table])
                self.col_dict[table] = sorted(old_cols | cols)
            else:
                self.col_dict[table] = sorted(cols)
            self.table_set.add(table)

    async def tables(self):
        if not self.table_set:
            t = await self.store.tables()
            self.table_set.update(t)
        return list(self.table_set)

    async def types(self):
        if not self.type_set:
            t = await self.store.types()
            self.type_set.update(t)
        return list(self.type_set)

    async def views(self):
        if not self.view_set:
            t = await self.store.views()
            self.view_set.update(t)
        return list(self.view_set)

    async def columns(self, table):
        if table not in self.col_dict:
            logger.debug('DBCache: fetching columns for "%s"', table)
            cols = await self.store.columns(table)
            self.col_dict[table] = cols
        else:
            logger.debug('DBCache: fetching columns for "%s" from cache', table)
            cols = self.col_dict[table]
        return cols

    async def schema(self, table):
        if table not in self.schema_dict:
            logger.debug('DBCache: fetching schema for "%s"', table)
            schema = await self.store.schema(table)
            self.schema_dict[table] = schema
        else:
            schema = self.schema_dict[table]
        return schema

    async def metadata(self, table):
        if not self.meta_dict:
            await self._get_metadata()
        return self.meta_dict.get(table)

    def _lookup_shortname(self, table, shortname):
        cols = self.meta_dict.get(table, {})
        return cols.get(shortname)

    def column_metadata(self, table, path):
        """Get DB column metadata for STIX object path `path`"""
        # 'path' here could be the prop side of a STIX object path,
        # BUT it could contain the "shortname" (column name)
        # We want to return the fullname too
        # parse_path, then use final node to look up longname
        # Strip obj_type from longname and replace with table:ref.
        if table == 'observed-data':
            longname = ''
        else:
            longname = f"{table}:"
        links = parse_prop(table, path)
        if len(links) > 1:
            # There's at least 1 reference/join
            longname_parts = []
            tgt_prop_parts = []
            tgt_type = table
            for link in links:
                if link[0] == 'rel':
                    # Store last referenced table as the "target" table
                    tgt_type = link[3]
                    longname_parts.append(link[2])
                else:
                    tgt_prop_parts.append(link[2])
            tgt_prop = '.'.join(tgt_prop_parts)
            longname += '.'.join(longname_parts) + '.'
            data = self._lookup_shortname(tgt_type, tgt_prop)
        else:
            data = self._lookup_shortname(table, path)
        if not data:
            if path.endswith('_refs'):  # Hack for reflists
                dtype = 'list'
            else:
                meta = prop_metadata(table, path)
                dtype = meta['dtype']
            data = {
                'otype': table,
                'path': path,
                'shortname': path,
                'dtype': dtype,
            }
        longname += data['path']
        logger.debug('column_metadata: %s -> %s (dtype %s)', path, longname, data['dtype'])
        return longname, dict(data)


async def get_dbcache(store: AsyncStorage):
    dbcache = AsyncDBCache(store)
    await dbcache.get_metadata()
    return dbcache
