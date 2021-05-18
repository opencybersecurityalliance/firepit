"""STIX and SQL identifier validators"""

import re

from firepit.exceptions import InvalidStixPath
from firepit.exceptions import InvalidViewname


def validate_name(name):
    if not bool(re.match(r'^[\w-]*$', name)):
        raise InvalidViewname(name)


def validate_path(path):
    if not bool(re.match(r"^([a-zA-Z][a-zA-Z0-9-]*:)?[\w\.'-]+$", path)):
        raise InvalidStixPath(path)
