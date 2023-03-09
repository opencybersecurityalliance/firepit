"""async local storage for firepit"""

import re

from importlib import import_module
from urllib.parse import urlparse

from firepit.aio.asyncwrapper import SyncWrapper
from firepit.validate import validate_name


def get_async_storage(connstring, session_id=None):
    """
    Get an async storage object for firepit.  `url` will determine the type; a file path means sqlite3.
    `session_id` is used in the case of postgresql to partition your data.
    """
    if session_id:
        validate_name(session_id)
    connstring = re.sub(r'^.*postgresql://', 'postgresql://', connstring)  # Ugly hack for kestrel
    url = urlparse(connstring)
    if url.scheme == 'postgresql':
        module = import_module('firepit.aio.asyncpgstorage')
        return module.get_storage(connstring, session_id)
    if url.scheme in ['sqlite3', '']:
        return SyncWrapper(url.path, session_id)
    raise NotImplementedError(url.scheme)
