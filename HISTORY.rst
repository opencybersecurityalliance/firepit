=======
History
=======

2.3.0 (2022-06-15)
------------------

- Added query.BinnedColumn so you can group by time buckets

2.2.0 (2022-06-08)
------------------

- Better STIX extension property support
  - Add a new `__columns` "private" table to store mapping from object path to column name
  - New path/prop metadata functions to supply metadata about STIX properties
- Improved STIX ``process`` "deterministic" ``id`` generation
  - Use a unique ID from extension properties, if found
  - Use related ``x-oca-asset`` hostname or ID if available

2.1.0 (2022-05-18)
------------------

- Add ``splint convert`` command to convert some logs files to STIX
  bundles

2.0.0 (2022-04-01)
------------------

- Use a "normalized" SQL database
- Initial STIX 2.1 support

1.3.0 (2021-10-04)
------------------

New assign_query API, minor query API improvements

- new way to create views via assign_query
- can now init a Query with a list instead of calling append
- Some SQL injection protection in query classes

1.2.0 (2021-08-18)
------------------

* Better support for grouped data

1.1.0 (2021-07-18)
------------------

* First stable release
* Concurrency fixes in ``cache()``

1.0.0 (2021-05-18)
------------------

* First release on PyPI.
