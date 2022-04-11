from .helpers import tmp_storage

def test_number_observed_url(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    number_observed = store.number_observed('url', 'value')
    assert number_observed == 31


def test_number_observed_ipv4(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    number_observed = store.number_observed('ipv4-addr', 'value')
    assert number_observed == 200
    number_observed = store.number_observed('ipv4-addr', 'value', '192.168.203.101')
    assert number_observed == 3

    store.extract('tens', 'ipv4-addr', 'q1',
                  "[ipv4-addr:value ISSUBSET '10.0.0.0/8']")
    number_observed = store.number_observed('tens', 'ipv4-addr:value')
    assert number_observed == 100
    number_observed = store.number_observed('tens', 'value', '10.0.0.73')
    assert number_observed == 14


def test_number_observed_src_dst(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    number_observed = store.number_observed('network-traffic', 'src_ref.value')
    assert number_observed == 100
    number_observed = store.number_observed('network-traffic', 'src_ref.value', '192.168.203.101')
    assert number_observed == 3

    store.extract('tens', 'network-traffic', 'q1',
                  "[network-traffic:dst_ref.value ISSUBSET '10.0.0.0/8']")
    number_observed = store.number_observed('tens', 'network-traffic:dst_ref.value')
    assert number_observed == 100
    number_observed = store.number_observed('tens', 'dst_ref.value', '10.0.0.73')
    assert number_observed == 14


def test_number_observed_ipv4_negative(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    number_observed = store.number_observed('ipv4-addr', 'value')
    assert number_observed == 200
    number_observed = store.number_observed('ipv4-addr', 'value', '9.9.9.9')
    assert number_observed == 0
