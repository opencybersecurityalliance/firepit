import pytest

from firepit.pgstorage import _rewrite_query


@pytest.mark.parametrize(
    'stmt, expected', [
        ('SELECT urls.value,     urls.id    FROM ((urls JOIN whatever))',
         'SELECT urls.* FROM ((urls JOIN whatever))'
        ),
        ('SELECT "observed-data".first_observed,     urls.value,     urls.id    FROM ((urls JOIN whatever))',
         'SELECT "observed-data".first_observed, urls.* FROM ((urls JOIN whatever))'
        ),
        ("SELECT file.name,     file.hashes.'SHA-1',     file.size     FROM whatever",
         'SELECT file.* FROM whatever'
        ),
        ('SELECT "network-traffic".id,     "network-traffic".src_port,     "network-traffic".src_ref.value     FROM whatever',
         'SELECT "network-traffic".* FROM whatever'
        ),
        ("SELECT DISTINCT process.pid,     process.name,     process.created,     process.creator_user_ref,     process.binary_ref,     process.id    FROM ((nt      JOIN __reflist ON ((nt.id = __reflist.target_ref)))      JOIN process ON ((__reflist.source_ref = process.id)))   WHERE (__reflist.ref_name = 'opened_connection_refs'::text)",
         "SELECT DISTINCT process.* FROM ((nt      JOIN __reflist ON ((nt.id = __reflist.target_ref)))      JOIN process ON ((__reflist.source_ref = process.id)))   WHERE (__reflist.ref_name = 'opened_connection_refs'::text)"
         ),
        ("  SELECT process.x_unique_id,     process.name,     process.binary_ref,     process.pid,     process.id,     process.parent_ref,     process.command_line,     process.created,     process.creator_user_ref    FROM process   WHERE (process.id IN ( SELECT process_1.id            FROM (process process_1              JOIN __queries ON ((process_1.id = __queries.sco_id)))           WHERE ((__queries.query_id = '9d832d67-c6a2-524a-8731-664f3b5c31c1'::text) AND (process_1.binary_ref IN ( SELECT file.id                    FROM file                   WHERE (file.name ~~ '%'::text)))))) UNION  SELECT process.x_unique_id,     process.name,     process.binary_ref,     process.pid,     process.id,     process.parent_ref,     process.command_line,     process.created,     process.creator_user_ref    FROM process   WHERE (process.id IN ( SELECT process_1.id            FROM (process process_1              JOIN __queries ON ((process_1.id = __queries.sco_id)))           WHERE (process_1.id = ANY (ARRAY['process--f65ff0e1-d2de-5c35-9796-b4ae0eafd5be'::text, 'process--c73f1f92-011a-51c6-967e-9b13b7efc8c7'::text, 'process--fcd0be5a-c109-5b45-999e-7b993f18d637'::text, 'process--69e78267-5a16-513a-b4e5-ecd8577dae1b'::text]))))",
         "SELECT process.* FROM process   WHERE (process.id IN ( SELECT process_1.id            FROM (process process_1              JOIN __queries ON ((process_1.id = __queries.sco_id)))           WHERE ((__queries.query_id = '9d832d67-c6a2-524a-8731-664f3b5c31c1'::text) AND (process_1.binary_ref IN ( SELECT file.id                    FROM file                   WHERE (file.name ~~ '%'::text)))))) UNION SELECT process.* FROM process   WHERE (process.id IN ( SELECT process_1.id            FROM (process process_1              JOIN __queries ON ((process_1.id = __queries.sco_id)))           WHERE (process_1.id = ANY (ARRAY['process--f65ff0e1-d2de-5c35-9796-b4ae0eafd5be'::text, 'process--c73f1f92-011a-51c6-967e-9b13b7efc8c7'::text, 'process--fcd0be5a-c109-5b45-999e-7b993f18d637'::text, 'process--69e78267-5a16-513a-b4e5-ecd8577dae1b'::text]))))"),
        ('  SELECT "observed-data".first_observed,     urls.value,     urls.id    FROM ((urls      JOIN __contains ON ((urls.id = __contains.target_ref)))      JOIN "observed-data" ON ((__contains.source_ref = "observed-data".id)))   ORDER BY "observed-data".first_observed',
         'SELECT "observed-data".first_observed, urls.* FROM ((urls      JOIN __contains ON ((urls.id = __contains.target_ref)))      JOIN "observed-data" ON ((__contains.source_ref = "observed-data".id)))   ORDER BY "observed-data".first_observed'),
        ('  SELECT "network-traffic".start,     "network-traffic"."end",     "network-traffic".src_ref,     "network-traffic".dst_ref,     "network-traffic".src_port,     "network-traffic".dst_port,     "network-traffic".protocols,     "network-traffic".src_byte_count,     "network-traffic".dst_byte_count,     "network-traffic".id    FROM "network-traffic"',
         'SELECT "network-traffic".* FROM "network-traffic"'),
    ]
)
def test_rewrite_query(stmt, expected):
    assert _rewrite_query(stmt) == expected
