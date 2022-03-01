from .helpers import tmp_storage


def test_group(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    store.group('conns', 'conns', by='src_ref.value')
    srcs = store.values('src_ref.value', 'conns')
    assert srcs

    groups = store.lookup('conns')
    assert groups
    assert 'unique_dst_port' in groups[0].keys()


def test_group_dst_port(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    store.group('conns', 'conns', by='dst_port')
    srcs = store.values('dst_port', 'conns')
    assert srcs

    groups = store.lookup('conns')
    assert groups
    assert 'dst_port' in groups[0].keys()


def test_group_src_dst(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    store.group('grp_conns', 'conns', by=['src_ref.value', 'dst_ref.value'])
    groups = store.lookup('grp_conns')
    assert len(groups) == 74


def test_group_src_aggs(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    store.group('grp_conns', 'conns', by=['src_ref.value'],
                aggs=[('COUNT', 'dst_ref.value', 'count')])
    groups = store.lookup('grp_conns')
    assert len(groups) == 53
    for group in groups:
        src = group['src_ref.value']
        if src == '192.168.216.111':
            assert group['count'] == 2
        elif src == '192.168.27.170':
            assert group['count'] == 2
        elif src == '192.168.70.186':
            assert group['count'] == 2
        elif src == '192.168.90.122':
            assert group['count'] == 6
        elif src == '192.168.95.234':
            assert group['count'] == 1


def test_group_src_dst_aggs(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])

    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    store.group('grp_conns', 'conns', by=['src_ref.value', 'dst_ref.value'],
                aggs=[('COUNT', '*', 'count')])
    groups = store.lookup('grp_conns')
    assert len(groups) == 74
    for group in groups:
        src = group['src_ref.value']
        dst = group['dst_ref.value']
        if src == '192.168.216.111' and dst == '10.0.0.197':
            assert group['count'] == 2
        elif src == '192.168.27.170' and dst == '10.0.0.214':
            assert group['count'] == 2
        elif src == '192.168.70.186' and dst == '10.0.0.139':
            assert group['count'] == 2
        elif src == '192.168.90.122' and dst == '10.0.0.214':
            assert group['count'] == 2
        else:
            assert group['count'] == 1
