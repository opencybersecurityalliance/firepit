import os
import pytest

from firepit.aio import get_async_storage
from firepit.aio.asyncstorage import AsyncDBCache
from firepit.exceptions import SessionExists, SessionNotFound

from .helpers import async_storage


@pytest.mark.asyncio
async def test_async_create(tmpdir):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session-create')
    store = get_async_storage(dbname, session)

    # First make sure the session doesn't already exists
    try:
        await store.attach()
        await store.delete()
    except SessionNotFound:
        pass

    # Now create it anew
    await store.create()

    # Creating it again should fail
    with pytest.raises(SessionExists):
        await store.create()


@pytest.mark.asyncio
async def test_async_attach_failure(tmpdir):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session-attach-fail')
    store = get_async_storage(dbname, session)

    with pytest.raises(SessionNotFound):
        await store.attach()


@pytest.mark.asyncio
async def test_async_basics(fake_bundle_file, tmpdir):
    store = await async_storage(tmpdir)
    await store.cache('q1', fake_bundle_file)
    assert 'url' in await store.tables()
    assert 'url' in await store.types()
    assert not await store._is_sql_view('url')
    url_table = await store.lookup('url')
    urls = {row['value'] for row in url_table}
    assert 'http://www8.example.com/page/176' in urls
    assert 'http://www27.example.com/page/64' in urls

    url_table = await store.lookup('url', ['value'])
    urls = {row['value'] for row in url_table}
    assert 'http://www8.example.com/page/176' in urls
    assert 'http://www27.example.com/page/64' in urls

    conns = await store.lookup('network-traffic',
                               ['src_ref.value', 'src_port', 'dst_ref.value', 'dst_port', 'protocols'])
    # Can't rely on any specific ordering here
    srcs = [conn['src_ref.value'] for conn in conns]
    assert '192.168.212.97' in srcs

    cache = AsyncDBCache(store)
    metadata = await cache.get_metadata()
    print(metadata)
    exp = {'observed-data', 'identity', 'url', 'network-traffic', 'ipv4-addr', 'user-account'}
    assert set(await cache.tables()) == exp
    assert set(await cache.types()) == exp
    assert await cache.views() == []
    assert set(await cache.columns('url')) == {'id', 'value'}

    await store.delete()


@pytest.mark.asyncio
async def test_cache_complex_object(tmpdir):
    bundle = {
        "type": "bundle",
        "id": "bundle--9e83faeb-3cb3-4aa2-97d0-35230c98e064",
        "objects": [
            {
                "type": "identity",
                "id": "identity--f431f809-377b-45e0-aa1c-6a4751cae5ff",
                "name": "example",
                "identity_class": "events"
            },
            {
                "id": "observed-data--cc5f37b9-b7bc-45b4-a3a0-99e2540a039b",
                "type": "observed-data",
                "created_by_ref": "identity--f431f809-377b-45e0-aa1c-6a4751cae5ff",
                "created": "2023-04-18T02:24:27.941Z",
                "modified": "2023-04-18T02:24:27.941Z",
                "objects": {
                    "0": {
                        "type": "x-oca-example",
                        "level_01": {
                            "level_02": {
                                "level_03": {
                                    "level_04": {
                                        "level_05": {
                                            "level_06": {
                                                "level_07": {
                                                    "level_08": {
                                                        "stuff": "It's a lot",
                                                        "things": [
                                                            {
                                                                "key": "key_1",
                                                                "value": "value_1"
                                                            },
                                                            {
                                                                "key": "key_2",
                                                                "value": "value_2"
                                                            },
                                                            {
                                                                "key": "key_3",
                                                                "value": "value_3"
                                                            }
                                                        ]
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ]
    }

    store = await async_storage(tmpdir)
    await store.cache('q1', bundle)
    cols = await store.columns('x-oca-example')

    # For custom objects we no longer flatten beyond first level.
    assert set(cols) == {'id', 'level_01'}
