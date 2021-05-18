import pytest

from firepit.props import auto_agg
from firepit.props import primary_prop


@pytest.mark.parametrize(
    'sco_type, expected', [
        ('directory', 'path'),
        ('file', 'name'),
        ('ipv4-addr', 'value'),
        ('ipv6-addr', 'value'),
        ('process', 'name'),
        ('url', 'value'),
        ('user-account', 'user_id'),
    ]
)
def test_primary_prop(sco_type, expected):
    assert primary_prop(sco_type) == expected


@pytest.mark.parametrize(
    'sco_type, prop, col_type, expected', [
        ('directory', 'path', 'TEXT', 'COUNT(DISTINCT "path") AS "unique_path"'),
        ('file', 'name', 'TEXT', 'COUNT(DISTINCT "name") AS "unique_name"'),
        ('file', 'first_observed', 'TEXT', 'MIN("first_observed") AS "first_observed"'),
        ('file', 'last_observed', 'TEXT', 'MAX("last_observed") AS "last_observed"'),
        ('file', 'number_observed', 'INTEGER', 'SUM("number_observed") AS "number_observed"'),
        ('file', 'hashes.MD5', 'TEXT', 'COUNT(DISTINCT "hashes.MD5") AS "unique_hashes.MD5"'),
        ('ipv4-addr', 'value', 'TEXT', 'COUNT(DISTINCT "value") AS "unique_value"'),
        ('ipv6-addr', 'value', 'TEXT', 'COUNT(DISTINCT "value") AS "unique_value"'),
        ('ipv6-addr', 'xf_risk', 'INTEGER', 'AVG("xf_risk") AS "mean_xf_risk"'),
        ('network-traffic', 'dst_bytes', 'INTEGER', 'AVG("dst_bytes") AS "mean_dst_bytes"'),
        ('network-traffic', 'dst_port', 'INTEGER', 'COUNT(DISTINCT "dst_port") AS "unique_dst_port"'),
        ('network-traffic', 'src_bytes', 'INTEGER', 'AVG("src_bytes") AS "mean_src_bytes"'),
        ('network-traffic', 'src_port', 'INTEGER', 'COUNT(DISTINCT "src_port") AS "unique_src_port"'),
        ('process', 'name', 'TEXT', 'COUNT(DISTINCT "name") AS "unique_name"'),
        ('process', 'pid', 'INTEGER', 'COUNT(DISTINCT "pid") AS "unique_pid"'),
        ('process', 'ppid', 'INTEGER', 'COUNT(DISTINCT "ppid") AS "unique_ppid"'),
        ('url', 'value', 'TEXT', 'COUNT(DISTINCT "value") AS "unique_value"'),
        ('url', 'id', 'TEXT', None),
        ('url', 'type', 'TEXT', None),
        ('url', 'x_contained_by_ref', 'TEXT', None),
        ('url', 'x_root', 'INTEGER', None),
        ('user-account', 'user_id', 'TEXT', 'COUNT(DISTINCT "user_id") AS "unique_user_id"'),
        ('ipv4-addr', 'xf_risk', 'bigint', 'AVG("xf_risk") AS "mean_xf_risk"'),
    ]
)
def test_auto_agg(sco_type, prop, col_type, expected):
    agg = auto_agg(sco_type, prop, col_type)
    assert agg == expected
