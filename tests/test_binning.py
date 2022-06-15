from firepit.query import Aggregation, BinnedColumn, Group, Order, Projection, Query, Table

from .helpers import tmp_storage


def test_bin_timestamp(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    query = Query([
        Table('observed-data'),
        Projection([BinnedColumn('first_observed', 2, 'm', alias='ts')]),
        Group(['ts']),
        Aggregation([('SUM', 'number_observed', 'count')]),
        Order(['ts']),
    ])
    results = store.run_query(query).fetchall()
    assert results[0]['ts'] == '2020-06-30T19:24:00Z'
    assert results[0]['count'] == 20
    assert results[1]['ts'] == '2020-06-30T19:26:00Z'
    assert results[1]['count'] == 42
    assert results[2]['ts'] == '2020-06-30T19:28:00Z'
    assert results[2]['count'] == 38


def test_bin_integer(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    query = Query([
        Table('network-traffic'),
        Projection([BinnedColumn('src_port', 10000, alias='port')]),
        Group(['port']),
        Aggregation([('COUNT', 'id', 'count')]),
        Order(['port']),
    ])
    results = store.run_query(query).fetchall()
    assert results[0]['port'] == 40000
    assert results[0]['count'] == 4
    assert results[1]['port'] == 50000
    assert results[1]['count'] == 69
    assert results[2]['port'] == 60000
    assert results[2]['count'] == 27


def test_bin_timestamp_via_group(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', fake_bundle_file)
    query = store.timestamped('network-traffic', run=False)
    store.assign_query('conn_ts', query, 'network-traffic')
    store.group('data', 'conn_ts',
                [BinnedColumn('first_observed', 2, 'm', alias='ts')],
                [('COUNT', 'id', 'count')])
    results = store.lookup('data')
    assert results[0]['ts'] == '2020-06-30T19:24:00Z'
    assert results[0]['count'] == 20
    assert results[1]['ts'] == '2020-06-30T19:26:00Z'
    assert results[1]['count'] == 42
    assert results[2]['ts'] == '2020-06-30T19:28:00Z'
    assert results[2]['count'] == 38
