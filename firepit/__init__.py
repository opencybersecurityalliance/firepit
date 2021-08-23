"""Top-level package for STIX Columnar Storage."""

__author__ = """IBM Security"""
__email__ = 'pcoccoli@us.ibm.com'
__version__ = '1.2.1'


from importlib import import_module
from urllib.parse import urlparse

from firepit.validate import validate_name


def get_storage(url, session_id=None):
    if session_id:
        validate_name(session_id)
    url = urlparse(url)
    if url.scheme == 'postgresql':
        module = import_module('firepit.pgstorage')
        return module.get_storage(url, session_id)
    if url.scheme in ['sqlite3', '']:
        module = import_module('firepit.sqlitestorage')
        return module.get_storage(url.path)
    raise NotImplementedError(url.scheme)
