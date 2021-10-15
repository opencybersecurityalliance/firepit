from .helpers import tmp_storage

def test_value_counts_url(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    value_counts = store.value_counts('url', 'value')
    assert len(value_counts) == 31


def test_value_counts_ipv4(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    value_counts = store.value_counts('ipv4-addr', 'value')
    assert len(value_counts) == 70
    data = {vc['value']: vc['count'] for vc in value_counts}
    assert data['192.168.203.101'] == 3

    store.extract('tens', 'ipv4-addr', 'q1',
                  "[ipv4-addr:value ISSUBSET '10.0.0.0/8']")
    value_counts = store.value_counts('tens', 'ipv4-addr:value')
    assert len(value_counts) == 10
    data = {vc['value']: vc['count'] for vc in value_counts}
    assert data['10.0.0.73'] == 14
