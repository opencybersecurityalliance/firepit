import os
from firepit import get_storage
from firepit.aio import get_async_storage
from firepit.exceptions import SessionExists, SessionNotFound


def tmp_storage(tmpdir, clear=True):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session')

    if clear:
        # Clear out previous test session
        store = get_storage(dbname, session)
        store.delete()

    return get_storage(dbname, session)


async def async_storage(tmpdir, clear=True):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session')
    store = get_async_storage(dbname, session)
    try:
        await store.create()
    except SessionExists:
        await store.attach()

    if clear:
        # Clear out previous test session
        try:
            await store.delete()
        except SessionNotFound as e:
            pass # nothing to delete
        await store.create()

    return store
