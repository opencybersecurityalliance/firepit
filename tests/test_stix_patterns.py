import pytest

from firepit.stix20 import stix2sql


def _normalize_ws(s):
    return ' '.join(s.split())


@pytest.mark.parametrize(
    'sco_type, pattern, where', [
        ('ipv4-addr', "[ipv4-addr:value = '9.9.9.9']", "\"value\" = '9.9.9.9'"),
        # Add in some unnecessary yet legal parens
        ('ipv4-addr', "[(ipv4-addr:value = '9.9.9.9')]", "(\"value\" = '9.9.9.9')"),
        # Type doesn't match pattern, so no WHERE clause
        ('process', "[ipv4-addr:value = '9.9.9.9']", ""),
        ('ipv4-addr', "[ipv4-addr:value ISSUBSET '192.168.0.0/16']", "(in_subnet(\"value\", '192.168.0.0/16'))"),
        ('domain-name', "[domain-name:value LIKE 'example.%']", "\"value\" LIKE 'example.%'"),
        ('url',
         "[url:value LIKE 'http://example.%' AND url:value LIKE '%.php']",
         "\"value\" LIKE 'http://example.%' AND \"value\" LIKE '%.php'"),
        ('url',
         "[url:value LIKE 'http://example.%' AND url:value LIKE '%.php' AND url:value LIKE '%foo%']",
         "\"value\" LIKE 'http://example.%' AND \"value\" LIKE '%.php' AND \"value\" LIKE '%foo%'"),
        ('url',
         "[(url:value LIKE 'http://example.%' OR url:value LIKE 'https://example.%') AND url:value LIKE '%foo%']",
         "(\"value\" LIKE 'http://example.%' OR \"value\" LIKE 'https://example.%') AND \"value\" LIKE '%foo%'"),
        # Need to handle reference lists
        ('network-traffic',
         "[network-traffic:protocols[*] = 'tcp']",
         "\"protocols\" LIKE '%tcp%'"),
        ('network-traffic',
         "[network-traffic:protocols[*] != 'tcp']",
         "\"protocols\" NOT LIKE '%tcp%'"),
        ('windows-registry-key',
         "[windows-registry-key:values[*].name = 'foo']",
         "\"values\" LIKE '%\"name\":\"foo\"%'"),
        ('network-traffic',
         "[network-traffic:src_ref.value = '127.0.0.1']",
         "\"src_ref\" IN (SELECT \"id\" FROM \"ipv4-addr\" WHERE \"value\" = '127.0.0.1')"),
        ('email-message',
         "[email-message:to_refs[*].value = 'name@example.com']",
         ("JOIN \"__reflist\" AS \"r\" ON \"email-message\".\"id\" = \"r\".\"source_ref\""
          " WHERE \"r\".\"target_ref\" IN (SELECT \"id\" FROM \"email-addr\" WHERE \"value\" = 'name@example.com')")),
        ('file',
         "[file:hashes.'SHA-256' = 'whatever']",
         "\"hashes.'SHA-256'\" = 'whatever'"),
        #TODO: need MATCHES example with PCRE that Python re doesn't support
    ]
)
def test_stix2sql(sco_type, pattern, where):
    assert where == _normalize_ws(stix2sql(pattern, sco_type))
