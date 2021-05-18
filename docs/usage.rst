=====
Usage
=====

As a package
------------

To use firepit in a project::

    from firepit.sqlstorage import get_storage

    db = get_storage('observations.db', session_id)
    db.cache('my_query_id', ['bundle1.json', 'bundle2.json'])

After caching your STIX bundles, your database will contain the data.

As a tool
---------

.. code-block:: bash

    Usage: firepit [OPTIONS] COMMAND [ARGS]...

      Columnar storage for STIX observations

    Options:
      --help  Show this message and exit.

    Commands:
      assign       Perform an operation on a column and name the result
      cache        Cache STIX observation data in SQL
      columns      Get the columns names of a view/table
      count        Get the count of rows in a view/table
      delete       Delete STIX observation data in SQL
      extract      Create a view of a subset of cached data
      filter       Create a filtered view of a subset of cached data
      get-appdata  Get the app-specific data for a view
      load         Cache STIX observation data in SQL
      lookup       Retrieve a view
      merge        Merge 2 or more views into a new view
      reassign     Update/replace STIX observation data in SQL
      schema       Get the schema of a view/table
      set-appdata  Set the app-specific data for a view
      sql          Run a SQL statement on the database [DANGEROUS!]
      tables       Get all view/table names
      type         Get the SCO type of a view/table
      values       Retrieve the values of a STIX object path from a view
      views        Get all view names

    $ firepit cache --help
    Usage: firepit cache [OPTIONS] QUERY_ID FILENAMES...

      Cache STIX observation data in SQL

    Arguments:
      QUERY_ID      An identifier for this set of data  [required]
      FILENAMES...  STIX bundle files of query results  [required]

    Options:
      --dbname TEXT   Path/name of database  [default: stix.db]
      --session TEXT  Session ID to data separation  [default: test-id]
      --help          Show this message and exit.
