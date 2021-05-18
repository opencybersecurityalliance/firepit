import os
import pytest


@pytest.fixture
def fake_bundle_file():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_bundle.json')


@pytest.fixture
def ccoe_bundle():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'ccoe_investigator_demo.json')


@pytest.fixture
def fake_csv_file():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_procs.csv')
