import os
from firepit import get_storage

def tmp_storage(tmpdir, clear=True):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session')

    if clear:
        # Clear out previous test session
        store = get_storage(dbname, session)
        store.delete()

    return get_storage(dbname, session)
