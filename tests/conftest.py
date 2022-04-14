import os
import pytest

collect_ignore = ['setup.py']


def tmp_storage(tmpdir, clear=True):
    dbname = os.getenv('FIREPITDB', str(tmpdir.join('test.db')))
    session = os.getenv('FIREPITID', 'test-session')

    if clear:
        # Clear out previous test session
        store = get_storage(dbname, session)
        store.delete()

    return get_storage(dbname, session)


@pytest.fixture
def fake_bundle_file():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_bundle.json')


@pytest.fixture
def fake_bundle_file_2():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_bundle_2.json')


@pytest.fixture
def ccoe_bundle():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'ccoe_investigator_demo.json')


@pytest.fixture
def fake_csv_file():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_procs.csv')


@pytest.fixture
def fake_bundle_list():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return [
        os.path.join(cwd, 'conn_a.json'),
        os.path.join(cwd, 'conn_b.json')
    ]


@pytest.fixture
def one_event_bundle():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'one_event.json')


@pytest.fixture
def mixed_v4_v6_bundle():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'mixed-v4-v6.json')
