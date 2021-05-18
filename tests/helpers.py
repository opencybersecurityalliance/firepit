from firepit.sqlitestorage import SQLiteStorage


def tmp_storage(tmpdir):
    return SQLiteStorage(str(tmpdir.join('test.db')))
