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
