import pytest

from firepit.raft import json_normalize


#@pytest.mark.skip(reason="WIP")
@pytest.mark.parametrize(
    'obj, expected', [
        ({'type': 'foo'}, {'type': 'foo'}),
        ({'type': 'file', 'hashes': {'SHA-1': 'abc123'}},
         {'type': 'file', "hashes.'SHA-1'": 'abc123'}),
        ({'type': 'foo', 'a': {'b': {'c': 1, 'd': 2}}}, {'type': 'foo', 'a.b.c': 1, 'a.b.d': 2}),
        ({'type': 'x-foo', 'a': {'b': {'c': 1, 'd': 2}}}, {'type': 'x-foo', 'a': {'b': {'c': 1, 'd': 2}}}),
        ({'type': 'x-foo', 'extensions': { 'x-cool-ext': {'a': 1, 'b': 2}}}, {'type': 'x-foo', "extensions.'x-cool-ext'.a": 1, "extensions.'x-cool-ext'.b": 2}),
    ]
)
def test_flatten_complex(obj, expected):
    assert json_normalize(obj) == expected
