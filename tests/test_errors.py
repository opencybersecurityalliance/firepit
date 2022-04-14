import os
import pytest

from firepit.exceptions import IncompatibleType
from firepit.exceptions import InvalidAttr
from firepit.exceptions import InvalidStixPath
from firepit.exceptions import InvalidViewname
from firepit.exceptions import StixPatternError
from firepit.exceptions import UnexpectedError
from firepit.query import Filter
from firepit.query import Group
from firepit.query import Predicate
from firepit.query import Query
from firepit.query import Table

from .helpers import tmp_storage


@pytest.fixture
def invalid_bundle_file():
    cwd = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd, 'test_error_bundle.json')


def test_local(invalid_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [invalid_bundle_file])


def test_extract_bad_stix_pattern(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    with pytest.raises(StixPatternError):
        store.extract('junk', 'ipv4-addr', 'q1', "whatever")


def test_filter_bad_stix_pattern(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(StixPatternError):
        store.filter('junk', 'url', 'urls', "value = 'http://www26.example.com/page/176'")


def test_filter_bad_input_view(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidViewname):
        store.filter('junk', 'url', 'urls OR 1', "[url:value = 'http://www26.example.com/page/176']")


def test_sqli_1(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidViewname):
        store.lookup('urls" UNION ALL SELECT * FROM "q1_url')


def test_sqli_2(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidAttr):
        store.values('url:fake.path', 'urls')
    with pytest.raises(InvalidStixPath):
        store.values('value" FROM "q1_ipv4-addr" UNION ALL SELECT "value', 'urls')


def test_sqli_3(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")

    res = store.load('test_urls', [
        {
            'type': 'url',
            'value': 'http://www26.example.com/page/176',
            'risk': 'high',
        },
        {
            'type': 'url',
            'value': 'http://www67.example.com/page/264',
            'risk': 'high',
        }
    ])

    with pytest.raises(InvalidViewname):
        store.join('sqli" AS SELECT * FROM "q1_url"; CREATE VIEW "marked',
                   'urls', 'value', 'test_urls', 'value')


def test_query_sqli_table(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidViewname):
        query = Query([Table('urls; select * from url; --')])
        store.run_query(query)


def test_query_sqli_predicate(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    query = Query('url')
    count = len(store.run_query(query).fetchall())
    assert count  # Make sure test is valid
    query = Query([
        Table('urls'),
        # This will not raise, but underlying SQL driver should prevent injection
        Filter([Predicate('value', '=', '1; select * from url; --')])
    ])
    result = store.run_query(query).fetchall()
    assert len(result) == 0  # If injection succeeded, we'd get len(result) == count


def test_empty_results(fake_bundle_file, tmpdir):
    """Look for finding objects that aren't there"""
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('my_findings', 'x-ibm-finding', 'q1', "[x-ibm-finding:name = 'Whatever']")
    findings = store.lookup('my_findings')
    assert findings == []


def test_lookup_bad_columns(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidAttr):
        store.lookup('urls', cols="1; select * from urls; --")


def test_lookup_bad_offset(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(ValueError):
        store.lookup('urls', offset="1; select * from urls; --")


def test_bad_groupby(fake_bundle_file, fake_csv_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('users', 'user-account', 'q1', "[ipv4-addr:value LIKE '10.%']")
    with pytest.raises(InvalidStixPath):
        store.assign('grouped_users', 'users', op='group',
                     by='1,extractvalue(0x0a,concat(0x0a,(select database())))--')


def test_assign_bad_columns(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(InvalidStixPath):
        store.assign('sorted', 'urls', op='sort',
                     by='value LIMIT 1; SELECT * FROM "urls"')


def test_sort_bad_limit(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('urls', 'url', 'q1', "[url:value LIKE '%page/1%']")
    with pytest.raises(ValueError):
        store.assign('sorted', 'urls', op='sort', by='value', limit='1; SELECT 1; --')


def test_merge_fail(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)

    store.cache('test-bundle', [fake_bundle_file])
    store.extract('urls', 'url', 'test-bundle', "[url:value LIKE '%page/1%']")
    store.extract('ips', 'ipv4-addr', 'test-bundle', "[ipv4-addr:value != '8.8.8.8']")

    with pytest.raises(IncompatibleType):
        store.merge('merged', ['urls', 'ips'])


def test_assign_query_sqli(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    query = Query([
        Table('url'),
        Filter([Predicate('value', '=', '1; select * from url; --')])
    ])
    # This no longer raises since the injection value is a valid string
    # The injection should not return any data, however.
    store.assign_query('urls', query)
    data = store.lookup('urls')
    assert len(data) == 0


def test_assign_query_sqli_quote(fake_bundle_file, tmpdir):
    # Same as previous test but includes a closing quote in the injection value
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    query = Query([
        Table('url'),
        Filter([Predicate('value', '=', '1\'; select * from url; --')])
    ])
    # Arguably, this shouldn't raise either: maybe we should escape the embedded quote?
    with pytest.raises(UnexpectedError):
        store.assign_query('urls', query)


def test_assign_query_sqli_group(fake_bundle_file, tmpdir):
    store = tmp_storage(tmpdir)
    store.cache('q1', [fake_bundle_file])
    store.extract('conns', 'network-traffic', 'q1', "[network-traffic:dst_port < 1024]")
    with pytest.raises(InvalidStixPath):
        query = Query([
            Table('conns'),
            Group(['src_ref.value where 1=2; select * from identity; --'])
        ])
