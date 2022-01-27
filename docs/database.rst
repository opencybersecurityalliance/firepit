========
Database
========

Supported Databases
-------------------

Firepit supports sqlite3 and PostgreSQL.

Database Tables
---------------

STIX observation data is inserted into multiple tables within a "session" (a database file for sqlite3 and a "schema" in PostgreSQL).  A table is created for each object type.  Since STIX data is a graph (i.e. nodes and edges), Firepit also creates some special "edge" tables:

- `__contains`: tracks which SCOs were contained in which `observed-data` SDOs
- `__reflist`: models 1:N reference lists like `process:opened_connection_refs`
- `__queries`: records which objects were inserted in which `cache` operations
- `__symtable`: records the name and type of "views" created by firepit calls

These tables are prefixed with `__` and considered "private" by firepit.

The `observed-data` Table
^^^^^^^^^^^^^^^^^^^^^^^^^

This tables contains the actual STIX Observed Data SDO that "conveys information about cyber security related entities such as files, systems, and networks using the STIX Cyber-observable Objects (SCOs)." [STIX-v2_1]_

This SDO (and therefore table) holds the timestamps and count of actual observations, whereas SCOs (and their firepit tables) only contain the properties (columns) of their respective object types.

The examples below show how to link `observed-data` with SCOs via the "private" `__contains` table.

Example SQL queries
-------------------

Timestamped SCOs
^^^^^^^^^^^^^^^^
To see the first 3 IP addresses observed:

.. code-block::

   sqlite> select obs.first_observed as time, sco.value as 'IP' 
      ...>  from "ipv4-addr" as sco
      ...>  join "__contains" as c on sco.id = c.target_ref
      ...>  join "observed-data" as obs on c.source_ref = obs.id
      ...>  order by time limit 3;
   time                      IP           
   ------------------------  -------------
   2019-11-16T12:55:28.101Z  192.168.1.156
   2019-11-16T12:55:28.101Z  192.168.1.1  
   2019-11-16T12:55:28.883Z  127.0.0.1

This is effectively equivalent to the CLI's `timestamped` command or the API's `timestamped` function:

.. code-block::

   $ firepit timestamped ipv4-addr value | head -5
   first_observed            value
   ------------------------  ---------------
   2019-11-16T12:55:28.101Z  192.168.1.156
   2019-11-16T12:55:28.101Z  192.168.1.1
   2019-11-16T12:55:28.883Z  127.0.0.1

Value counts
^^^^^^^^^^^^

To get a count of observations of each IP address (the `sqlite3` CLI truncates the `value` column):

.. code-block::

   sqlite> select sco.value, count(*) from "ipv4-addr" as sco
      ...>  join "__contains" as c on sco.id = c.target_ref
      ...>  join "observed-data" as obs on c.source_ref = obs.id
      ...>  group by sco.value;
   value       count(*)  
   ----------  ----------
   127.0.0.1   413       
   172.16.0.1  33        
   172.16.0.1  7         
   172.16.0.1  8         
   172.16.0.1  24        
   172.16.0.2  13        
   192.168.1.  166       
   192.168.1.  138       
   192.168.1.  1         
   192.168.1.  3         
   192.168.1.  4         
   192.168.17  8         
   192.168.17  1         
   192.168.17  4         
   192.168.23  10        
   192.168.23  2         
   192.168.23  1         
   192.168.23  4

Again, this operation is provided by the CLI's `value-counts` command or the API's `value_counts` function:

.. code-block::

   $ firepit value-counts ipv4-addr value
   value              count
   ---------------  -------
   127.0.0.1            413
   172.16.0.100          33
   172.16.0.101           7
   172.16.0.104           8
   172.16.0.112          24
   172.16.0.255          13
   192.168.1.1          166
   192.168.1.156        138
   192.168.1.163          1
   192.168.1.169          3
   192.168.1.255          4
   192.168.175.1          8
   192.168.175.254        1
   192.168.175.255        4
   192.168.232.1         10
   192.168.232.2          2
   192.168.232.254        1
   192.168.232.255        4

.. [STIX-v2_1] STIX Version 2.1. Edited by Bret Jordan, Rich Piazza, and Trey Darley. 10 June 2021. OASIS Standard. https://docs.oasis-open.org/cti/stix/v2.1/os/stix-v2.1-os.html. Latest stage: https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html.
