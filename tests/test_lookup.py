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
