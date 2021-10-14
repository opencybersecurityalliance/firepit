from .helpers import tmp_storage

def test_load_evolving(tmpdir):
    store = tmp_storage(tmpdir)    
    ips = [
        {"value": "10.0.0.1"},
        {"value": "10.0.0.1", "x_extra": 1},
    ]
    res = store.load('test_ips', ips, sco_type='ipv4-addr')
    assert res == 'ipv4-addr'
    rows = store.lookup('test_ips')
    assert len(rows) == 1
    assert rows[0]['type'] == 'ipv4-addr'
    assert rows[0]['value'] == '10.0.0.1'
    assert rows[0]['x_extra'] == 1    


def test_load_missing(tmpdir):
    store = tmp_storage(tmpdir)
    ips = [
        {"value": "10.0.0.1", "x_extra": 99},
        {"value": "10.0.0.1"},
    ]
    res = store.load('test_ips', ips, sco_type='ipv4-addr')
    assert res == 'ipv4-addr'
    rows = store.lookup('test_ips')
    assert len(rows) == 1
    assert rows[0]['type'] == 'ipv4-addr'
    assert rows[0]['value'] == '10.0.0.1'
    assert rows[0]['x_extra'] == 99
