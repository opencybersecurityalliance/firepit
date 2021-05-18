from .helpers import tmp_storage


def test_x_oca_event(ccoe_bundle, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [ccoe_bundle])

    store.extract('events', 'x-oca-event', 'q1', "[x-oca-event:kind = 'event']")
    print(store.columns('events'))
    assert 'process_ref.id' in store.columns('events')
