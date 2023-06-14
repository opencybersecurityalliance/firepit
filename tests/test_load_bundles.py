import ast
import csv

from collections import Counter
from decimal import Decimal

import pytest

from firepit.exceptions import UnknownViewname
from firepit.query import Aggregation
from firepit.query import Column
from firepit.query import Group
from firepit.query import Join
from firepit.query import Limit
from firepit.query import Query
from firepit.query import Order
from firepit.query import Projection
from firepit.query import Table

from .helpers import tmp_storage


def test_local(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)

    store.cache('q1', fake_bundle_file)
    assert 'url' in store.tables()
    assert 'url' in store.types()
    assert not store._is_sql_view('url')

    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    assert 'urls' in store.views()
    assert 'urls' not in store.types()
    assert store._is_sql_view('urls')
    urls = store.values('url:value', 'urls')
    print(urls)
    assert len(urls) == 14
    assert 'http://www8.example.com/page/176' in urls
    assert 'http://www27.example.com/page/64' not in urls


