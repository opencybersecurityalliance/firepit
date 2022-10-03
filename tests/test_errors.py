import os
import pytest

from firepit.exceptions import IncompatibleType
from firepit.exceptions import InvalidAttr
from firepit.exceptions import InvalidObject
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


def test_empty_type(tmpdir):
    store = tmp_storage(tmpdir)

    # Some actual crap I've seen
    b1 = {
        "type": "bundle",
        "id": "bundle--0911b0a3-7a32-4bd5-bddd-5757bd87e8a0",
        "objects": [
            {
                "type": "identity",
                "id": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.304Z",
                "modified": "2020-06-30T19:31:23.304Z",
                "name": "test",
                "identity_class": "organization"
            },
            {
                "type": "",  # Clearly not valid
                "id": "27f23ce-93de-4ee3-8dd1-cbb4e5b005cd",
                "value": "foo"
            },
            {
                "spec_version": "2.1",
                "type": "observed-data",
                "id": "observed-data--4bd9c203-a327-4b81-b2fa-e6fc8d705dcc",
                "created_by_ref": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.651Z",
                "modified": "2020-06-30T19:31:23.651Z",
                "first_observed": "2020-06-30T19:25:09.447726Z",
                "last_observed": "2020-06-30T19:28:49.692424Z",
                "number_observed": 1,
                "object_refs": [
                    "27f23ce-93de-4ee3-8dd1-cbb4e5b005cd",
                ]
            }
        ]
    }
    with pytest.raises(InvalidObject):
        store.cache('b1', b1)


def test_missing_type(tmpdir):
    store = tmp_storage(tmpdir)

    # Some actual crap I've seen
    b1 = {
        "type": "bundle",
        "id": "bundle--0911b0a3-7a32-4bd5-bddd-5757bd87e8a0",
        "objects": [
            {
                "type": "identity",
                "id": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.304Z",
                "modified": "2020-06-30T19:31:23.304Z",
                "name": "test",
                "identity_class": "organization"
            },
            {
                "id": "27f23ce-93de-4ee3-8dd1-cbb4e5b005cd",
                "value": "foo"
            },
            {
                "spec_version": "2.1",
                "type": "observed-data",
                "id": "observed-data--4bd9c203-a327-4b81-b2fa-e6fc8d705dcc",
                "created_by_ref": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.651Z",
                "modified": "2020-06-30T19:31:23.651Z",
                "first_observed": "2020-06-30T19:25:09.447726Z",
                "last_observed": "2020-06-30T19:28:49.692424Z",
                "number_observed": 1,
                "object_refs": [
                    "27f23ce-93de-4ee3-8dd1-cbb4e5b005cd",
                ]
            }
        ]
    }
    with pytest.raises(InvalidObject):
        store.cache('b1', b1)


def test_non_str_id(tmpdir):
    store = tmp_storage(tmpdir)

    # Some actual crap I've seen
    class MyId:
        oid: str = None

        def __init__(self, oid_):
            self.oid = oid_

        def __str__(self):
            return self.oid

    my_id = MyId("ipv4-addr--3b9b974c-af2e-55e2-921d-647f8777356e")
    b1 = {
        "type": "bundle",
        "id": "bundle--0911b0a3-7a32-4bd5-bddd-5757bd87e8a0",
        "objects": [
            {
                "type": "identity",
                "id": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.304Z",
                "modified": "2020-06-30T19:31:23.304Z",
                "name": "test",
                "identity_class": "organization"
            },
            {
                "spec_version": "2.1",
                "type": "ipv4-addr",
                "id": my_id,
                "value": "198.51.100.123"
            },
            {
                "spec_version": "2.1",
                "type": "observed-data",
                "id": "observed-data--4bd9c203-a327-4b81-b2fa-e6fc8d705dcc",
                "created_by_ref": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.651Z",
                "modified": "2020-06-30T19:31:23.651Z",
                "first_observed": "2020-06-30T19:25:09.447726Z",
                "last_observed": "2020-06-30T19:28:49.692424Z",
                "number_observed": 1,
                "object_refs": [
                    my_id
                ]
            }
        ]
    }
    store.cache('b1', b1)
