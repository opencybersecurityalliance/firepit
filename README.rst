===============================
Firepit - STIX Columnar Storage
===============================


.. image:: https://img.shields.io/pypi/v/firepit.svg
        :target: https://pypi.python.org/pypi/firepit

.. image:: https://readthedocs.org/projects/firepit/badge/?version=latest
        :target: https://firepit.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://github.com/opencybersecurityalliance/firepit/actions/workflows/testing.yml/badge.svg
        :target: https://github.com/opencybersecurityalliance/firepit
        :alt: Unit Test Status

.. image:: https://codecov.io/gh/opencybersecurityalliance/firepit/branch/develop/graph/badge.svg?token=Pu7pkqmE5W
        :target: https://codecov.io/gh/opencybersecurityalliance/firepit


Columnar storage for STIX 2.0 observations.


* Free software: Apache Software License 2.0
* Documentation: https://firepit.readthedocs.io.


Features
--------

* Transforms STIX Observation SDOs to a columnar format
* Inserts those transformed observations into SQL (currently sqlite3 and PostgreSQL)

Motivation
----------

`STIX 2.0 JSON <https://docs.oasis-open.org/cti/stix/v2.0/stix-v2.0-part1-stix-core.html>`_ is a graph-like data format.  There aren't many popular tools for working with graph-like data, but there are numerous tools for working with data from SQL databases.  Firepit attempts to make those tools usable with STIX data obtained from `stix-shifter <https://github.com/opencybersecurityalliance/stix-shifter>`_.

Firepit also supports `STIX 2.1 <https://docs.oasis-open.org/cti/stix/v2.1/os/stix-v2.1-os.html>`_

Firepit is primarily designed for use with the `Kestrel Threat Hunting Language <https://github.com/opencybersecurityalliance/kestrel-lang>`_.

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
