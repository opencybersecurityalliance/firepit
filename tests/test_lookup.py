from collections import Counter

from .helpers import tmp_storage


def test_lookup(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    conns = store.lookup('conns')
    assert len(conns) == 78


def test_lookup_dst_port(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    ports = store.lookup('conns', cols=['dst_port'])
    assert len(ports) == 78
    assert set([i['dst_port'] for i in ports]) == {22, 80, 514}


def test_lookup_src_dst(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    conns = store.lookup('conns', cols=['src_ref.value', 'dst_ref.value'])
    assert len(conns) == 78
    counter = Counter([c['src_ref.value'] + '_' + c['dst_ref.value'] for c in conns])
    assert counter['192.168.90.122_10.0.0.214'] == 2
    assert counter['192.168.132.245_10.0.0.214'] == 1


def test_lookup_mixed(mixed_v4_v6_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [mixed_v4_v6_bundle])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port > 0]")
    conns = store.lookup('conns')
    assert len(conns) == 10
    counter = Counter([c['src_ref.value'] + '_' + c['dst_ref.value'] for c in conns])
    assert counter['192.168.1.156_192.168.1.1'] == 2
    assert counter['fe80:0:0:0:5d67:4a8:1e69:54d8_fe80:0:0:0:950c:ff99:129:5107'] == 1


def test_lookup_procs(ccoe_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [ccoe_bundle])

    store.extract('procs', 'process', 'q1', "[process:pid > 0]")
    procs = store.lookup('procs')
    assert len(procs) == 1021
    assert 'parent_ref.pid' in procs[0]
