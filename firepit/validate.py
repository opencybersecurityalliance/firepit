"""STIX and SQL identifier validators"""

import re

from firepit.exceptions import InvalidStixPath
from firepit.exceptions import InvalidViewname

NAME_PATTERN = r'^[\w-]*$'
PATH_PATTERN = r"^([a-zA-Z][a-zA-Z0-9-]*:)?[\w\'-]+(\[\*\])?(\.[\w\'-]+)*$"

def validate_name(name):
    """
    Make sure `name` is a valid (SQL) identifier
    """
    if not isinstance(name, str) or not bool(re.match(NAME_PATTERN, name)):
        raise InvalidViewname(name)


def validate_path(path):
    """
    Make sure `path` is a valid STIX object path or property name
    """
    if (not isinstance(path, str) or
        not bool(re.match(PATH_PATTERN, path))):
        raise InvalidStixPath(path)
