=====
Usage
=====

As a package
------------

To use firepit in a project::

    from firepit import get_storage

    db = get_storage('observations.db', session_id)
    db.cache('my_query_id', ['bundle1.json', 'bundle2.json'])

After caching your STIX bundles, your database will contain the data.

Passing a file path to `get_storage` will use sqlite3.  Passing in a
PostgreSQL connection URI (e.g. postgresql://...) will instead
attempt to connect to the PostgreSQL instance specified.


As a tool
---------

You can use the `firepit` command line tool to ingest and query your data.

To make things easier, you can set a pair of environment variables:

.. code-block::

   export FIREPITDB=my_dbname
   export FIREPITID=my_session_id

`FIREPITDB` is your DB filename (sqlite3) or connection URI
(PostgreSQL).  `FIREPITID` is a "session" ID you can use to keep your
data organized.


.. code-block::

    $ firepit --help
    Usage: firepit [OPTIONS] COMMAND [ARGS]...

      Columnar storage for STIX observations

    Options:
      --dbname TEXT   Path/name of database  [default: stix.db]
      --session TEXT  Session ID to data separation  [default: test-id]
      --help          Show this message and exit.

    Commands:
      assign       Perform an operation on a column and name the result
      cache        Cache STIX observation data in SQL
      columns      Get the columns names of a view/table
      count        Get the count of rows in a view/table
      delete       Delete STIX observation data in SQL
      extract      Create a view of a subset of cached data
      filter       Create a filtered view of a subset of cached data
      get-appdata  Get the app-specific data for a view
      join         Join two views
      load         Cache STIX observation data in SQL
      lookup       Retrieve a view
      merge        Merge 2 or more views into a new view
      reassign     Update/replace STIX observation data in SQL
      remove       Remove a view
      rename       Rename a view
      schema       Get the schema of a view/table
      set-appdata  Set the app-specific data for a view
      sql          Run a SQL statement on the database [DANGEROUS!]
      tables       Get all view/table names
      type         Get the SCO type of a view/table
      values       Retrieve the values of a STIX object path from a view
      viewdata     Get view data for views [default is all views]
      views        Get all view names

    $ firepit cache --help
    Usage: firepit cache [OPTIONS] QUERY_ID FILENAMES...

      Cache STIX observation data in SQL

    Arguments:
      QUERY_ID      An identifier for this set of data  [required]
      FILENAMES...  STIX bundle files of query results  [required]

    Options:
      --help  Show this message and exit.
