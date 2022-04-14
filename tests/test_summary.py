from .helpers import tmp_storage


def test_summary_url(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    summary = store.summary('url', 'value')
    assert summary['first_observed'] == '2020-06-30T19:25:10.723267Z'
    assert summary['last_observed'] == '2020-06-30T19:29:59.916295Z'
    assert summary['number_observed'] == 31


def test_summary_ipv4(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    summary = store.summary('ipv4-addr', 'value')
    assert summary['first_observed'] == '2020-06-30T19:25:09.447726Z'
    assert summary['last_observed'] == '2020-06-30T19:29:59.96346Z'
    assert summary['number_observed'] == 200
    summary = store.summary('ipv4-addr', 'value', '192.168.203.101')
    assert summary['first_observed'] == '2020-06-30T19:26:18.788238Z'
    assert summary['last_observed'] == '2020-06-30T19:28:23.940523Z'
    assert summary['number_observed'] == 3

    store.extract('tens', 'ipv4-addr', 'q1',
                  "[ipv4-addr:value ISSUBSET '10.0.0.0/8']")
    summary = store.summary('tens', 'ipv4-addr:value')
    assert summary['first_observed'] == '2020-06-30T19:25:09.447726Z'
    assert summary['last_observed'] == '2020-06-30T19:29:59.96346Z'
    assert summary['number_observed'] == 100
    summary = store.summary('tens', 'value', '10.0.0.73')
    assert summary['first_observed'] == '2020-06-30T19:25:24.15486Z'
    assert summary['last_observed'] == '2020-06-30T19:29:49.549512Z'
    assert summary['number_observed'] == 14


def test_summary_src_dst(tmpdir, fake_bundle_file):
    store = tmp_storage(tmpdir)    
    store.cache('q1', fake_bundle_file)
    summary = store.summary('network-traffic', 'src_ref.value')
    assert summary['first_observed'] == '2020-06-30T19:25:09.447726Z'
    assert summary['last_observed'] == '2020-06-30T19:29:59.96346Z'
    assert summary['number_observed'] == 100
    summary = store.summary('network-traffic', 'src_ref.value', '192.168.203.101')
    assert summary['first_observed'] == '2020-06-30T19:26:18.788238Z'
    assert summary['last_observed'] == '2020-06-30T19:28:23.940523Z'
    assert summary['number_observed'] == 3

    store.extract('tens', 'network-traffic', 'q1',
                  "[network-traffic:dst_ref.value ISSUBSET '10.0.0.0/8']")
    summary = store.summary('tens', 'network-traffic:dst_ref.value')
    assert summary['first_observed'] == '2020-06-30T19:25:09.447726Z'
    assert summary['last_observed'] == '2020-06-30T19:29:59.96346Z'
    assert summary['number_observed'] == 100
    summary = store.summary('tens', 'dst_ref.value', '10.0.0.73')
    assert summary['first_observed'] == '2020-06-30T19:25:24.15486Z'
    assert summary['last_observed'] == '2020-06-30T19:29:49.549512Z'
    assert summary['number_observed'] == 14
