from collections import Counter

from firepit.deref import auto_deref

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


def test_deref_paths(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")

    joins, proj = auto_deref(store, 'conns', paths=['src_ref.value'])
    assert len(joins) == 2
    assert len(proj.cols) == 1
    assert str(proj.cols[0]) == '"src_ref"."value" AS "src_ref.value"'
