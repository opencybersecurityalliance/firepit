import uuid
import pytest

from firepit.exceptions import InvalidStixPath
from firepit.exceptions import InvalidViewname
from firepit.validate import validate_name
from firepit.validate import validate_path


@pytest.mark.parametrize(
    'name, expected', [
        ('foo', True),
        ('[*]', False),
        ('__tmp_6668fcc6300f40e39c255c6573d79180', True),
        ('__tmp_' + uuid.uuid4().hex, True),
        ('foo;', False),
        ('foo; --', False),
    ]
)
def test_validate_name(name, expected):
    if expected:
        validate_name(name)
    else:
        with pytest.raises(InvalidViewname):
            validate_name(name)


@pytest.mark.parametrize(
    'path, expected', [
        ('foo', True),
        # ('things[*]', True),
        ('one.two', True),
        ("hashes.'SHA-256'", True),
        ('foo;', False),
        ('foo; --', False),
    ]
)
def test_validate_path(path, expected):
    if expected:
        validate_path(path)
    else:
        with pytest.raises(InvalidStixPath):
            validate_path(path)
