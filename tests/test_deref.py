from firepit.deref import auto_deref, auto_deref_cached
from firepit.sqlstorage import _get_col_dict
from .helpers import tmp_storage


def test_deref(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")

    # Note strange inconsistency in return val types - FIXME?
    # (List[Join], Projection)
    joins, proj = auto_deref(store, 'conns')
    assert len(joins) == 2
    # After deref, we added value and id cols but took away src_ref, dst_ref
    assert len(proj.cols) == len(store.columns('conns')) - 2 + 2 * 2


def test_deref_cached(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")

    # Note strange inconsistency in return val types - FIXME?
    # (List[Join], Projection)
    cols = store.columns('conns')
    joins, proj = auto_deref_cached('conns', cols, _get_col_dict(store))
    assert len(joins) == 2
    # After deref, we added value and id cols but took away src_ref, dst_ref
    assert len(proj.cols) == len(store.columns('conns')) - 2 + 2 * 2


def test_deref_paths(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")

    joins, proj = auto_deref(store, 'conns', paths=['src_ref.value'])
    assert len(joins) == 1
    assert len(proj.cols) == 1
    assert str(proj.cols[0]) == '"src_ref"."value" AS "src_ref.value"'


def test_deref_cached_paths(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")

    cols = store.columns('conns')
    joins, proj = auto_deref_cached('conns', cols, _get_col_dict(store), paths=['src_ref.value'])
    assert len(joins) == 1
    assert len(proj.cols) == 1
    assert str(proj.cols[0]) == '"src_ref"."value" AS "src_ref.value"'


def test_deref_mixed(mixed_v4_v6_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [mixed_v4_v6_bundle])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    conns = store.lookup('conns')

    # Simulate running some analytics to enrich these
    for conn in conns:
        conn['src_ref.x_enrich'] = 1
        conn['dst_ref.x_enrich'] = 1

    # Now reload into the same var
    store.reassign('conns', conns)
    cols = store.columns('conns')
    joins, proj = auto_deref_cached('conns', cols, _get_col_dict(store))
    assert len(joins) == 2 * 2  # (v4, v6) X (src, dst)
    result_cols = {str(col) for col in proj.cols}
    print(result_cols)
    # Only some v4 connections got enriched
    assert 'COALESCE(src_ref4.value, src_ref6.value) AS "src_ref.value"' in result_cols
    assert 'COALESCE(src_ref4.id, src_ref6.id) AS "src_ref.id"' in result_cols
    assert '"src_ref4"."x_enrich" AS "src_ref.x_enrich"' in result_cols
    assert 'COALESCE(dst_ref4.value, dst_ref6.value) AS "dst_ref.value"' in result_cols
    assert 'COALESCE(dst_ref4.id, dst_ref6.id) AS "dst_ref.id"' in result_cols
    assert '"dst_ref4"."x_enrich" AS "dst_ref.x_enrich"' in result_cols
