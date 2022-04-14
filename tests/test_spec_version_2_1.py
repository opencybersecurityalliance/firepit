import os

import pytest

from .helpers import tmp_storage


@pytest.fixture
def bundle_21():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'spec_2_1_bundle.json')


def test_stix_21(bundle_21, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [bundle_21])

    types =  store.types()
    assert 'identity' in types
    assert 'domain-name' in types
    assert 'ipv4-addr' in types

    cols = store.columns('domain-name')
    assert 'type' not in cols
    assert 'spec_version' not in cols
    data = store.lookup('domain-name')
    assert len(data) == 1
    assert data[0]['id'] == 'domain-name--bedb4899-d24b-5401-bc86-8f6b4cc18ec7'
    assert data[0]['value'] == 'example.com'

    cols = store.columns('ipv4-addr')
    assert 'type' not in cols
    assert 'spec_version' not in cols
    data = store.lookup('ipv4-addr')
    assert len(data) == 1
    assert data[0]['id'] == 'ipv4-addr--28bb3599-77cd-5a82-a950-b5bc3caf07c4'
    assert data[0]['value'] == '198.51.100.3'

    store.extract('domains', 'domain-name', 'q1', "[domain-name:value LIKE '%.com']")
    data = store.values('domain-name:value', 'domains')
    print(data)
    assert len(data) == 1
    assert data[0] == 'example.com'
    assert store.count('domains') == 1
    value_counts = store.value_counts('domains', 'value')
    assert len(value_counts) == 1
    assert value_counts[0]['value'] == 'example.com'
    assert value_counts[0]['count'] == 1

    store.extract('ips', 'ipv4-addr', 'q1', "[ipv4-addr:value ISSUBSET '198.51.100.0/24']")
    data = store.values('ipv4-addr:value', 'ips')
    print(data)
    assert len(data) == 1
    assert data[0] == '198.51.100.3'
    assert store.count('ips') == 1
    value_counts = store.value_counts('ips', 'value')
    assert len(value_counts) == 1
    assert value_counts[0]['value'] == '198.51.100.3'
    assert value_counts[0]['count'] == 1
