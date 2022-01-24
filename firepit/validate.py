"""STIX and SQL identifier validators"""

import re

from firepit.exceptions import InvalidStixPath
from firepit.exceptions import InvalidViewname


def validate_name(name):
    """
    Make sure `name` is a valid (SQL) identifier
    """
    if not bool(re.match(r'^[\w-]*$', name)):
        raise InvalidViewname(name)


def validate_path(path):
    """
    Make sure `path` is a valid STIX object path or property name
    """
    if not bool(re.match(r"^([a-zA-Z][a-zA-Z0-9-]*:)?[\w\.'-]+$", path)):
        raise InvalidStixPath(path)
