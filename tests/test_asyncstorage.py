import os
import pytest
from urllib.parse import urlparse

from firepit.asyncstorage import AsyncStorage
from firepit.asyncstorage import SyncWrapper
from firepit.exceptions import SessionNotFound


async def async_storage(tmpdir, clear=True):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session')
    store = AsyncStorage(dbname, session)
    url = urlparse(dbname)
    if url.scheme == 'postgresql':
        store = AsyncStorage(dbname, session)
    if url.scheme in ['sqlite3', '']:
        store = SyncWrapper(url.path, session)
    await store.attach()

    if clear:
        # Clear out previous test session
        try:
            await store.delete()
        except SessionNotFound as e:
            pass # nothing to delete
        await store.create()

    return store


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

    await store.delete()
