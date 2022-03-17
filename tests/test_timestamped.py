from .helpers import tmp_storage


def test_timestamped_url(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    timestamped = store.timestamped('url', 'value')
    assert len(timestamped) == 31


def test_timestamped_url_only(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    timestamped = store.timestamped('url', ['value'])
    assert len(timestamped) == 31
    assert set(timestamped[0].keys()) == {'first_observed', 'value'}


def test_timestamped_ipv4(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    timestamped = store.timestamped('ipv4-addr', 'value')
    assert len(timestamped) == 200
    timestamped = store.timestamped('ipv4-addr', 'value', '192.168.203.101')
    assert len(timestamped) == 3

    store.extract('tens', 'ipv4-addr', 'q1',
                  "[ipv4-addr:value ISSUBSET '10.0.0.0/8']")
    timestamped = store.timestamped('tens', 'ipv4-addr:value')
    assert len(timestamped) == 100
    timestamped = store.timestamped('tens', 'value', '10.0.0.73')
    assert len(timestamped) == 14


def test_timestamped_src_dst(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    timestamped = store.timestamped('network-traffic', 'src_ref.value')
    assert len(timestamped) == 100
    timestamped = store.timestamped('network-traffic', 'src_ref.value', '192.168.203.101')
    assert len(timestamped) == 3

    store.extract('tens', 'network-traffic', 'q1',
                  "[network-traffic:dst_ref.value ISSUBSET '10.0.0.0/8']")
    timestamped = store.timestamped('tens', 'network-traffic:dst_ref.value')
    assert len(timestamped) == 100
    timestamped = store.timestamped('tens', 'dst_ref.value', '10.0.0.73')
    assert len(timestamped) == 14
    
