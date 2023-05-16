import json
import pytest

import numpy as np
import pandas as pd

from firepit.aio.ingest import ingest
from firepit.aio.ingest import translate

from .helpers import async_storage


# Data source is a STIX Identity SDO
ts = '2023-01-30T16:34:17.784Z'
data_source = {
    'id': 'identity--97e0ed39-5cf3-4daf-94cd-06087221db32',
    'name': 'test',
    'identity_class': 'test',
    'created': ts,
    'modified': ts,
    'type': 'identity'
}


# Adapted from stix-shifter sources
class ToLowercaseArray:
    """A value transformer for expected array values"""

    @staticmethod
    def transform(obj):
        try:
            obj_array = obj if isinstance(obj, list) else obj.split(', ')
            # Loop through entries inside obj_array and make all strings lowercase to meet STIX format
            obj_array = [entry.lower() for entry in obj_array]
            return obj_array
        except:
            pass


def test_translate():
    # Example STIX mapping
    stix_map = {
        "timestamp": [
            {
                "key": "first_observed",
                "transformer": "EpochToTimestamp",
                "cybox": False
            },
            {
                "key": "last_observed",
                "transformer": "EpochToTimestamp",
                "cybox": False
            }
        ],
        "ip": [  # elastic_ecs does this
            {
                "key": "ipv4-addr.value",
                "object": "host_ip",
                "unwrap": True,
                "transformer": "FilterIPv4List"
            },
            {
                "key": "ipv6-addr.value",
                "object": "host_ipv6",
                "unwrap": True,
                "transformer": "FilterIPv6List"
            },
            {
                "key": "x-oca-asset.ip_refs",
                "object": "host",
                "references": ["host_ip", "host_ipv6"],
                "unwrap": True
            }
        ],
        "mac": [
            {
                "key": "mac-addr.value",
                "object": "host_mac",
                "unwrap": True
            },
            {
                "key": "x-oca-asset.mac_refs",
                "object": "host",
                "references": "host_mac",
                "unwrap": True
            }
        ],
        "sourceip": [
            {
                "key": "ipv4-addr.value",
                "object": "src_ip"
            },
            {
                "key": "ipv6-addr.value",
                "object": "src_ip"
            },
            {
                "key": "network-traffic.src_ref",
                "object": "nt",
                "references": "src_ip"
            }
        ],
        "sourceport": {
            "key": "network-traffic.src_port",
            "object": "nt",
        },
        "process": {
            "command_line": {
                "key": "process.command_line",
                "object": "process"
            },
            "entity_id": {
                "key": "process.x_unique_id",
                "object": "process"
            },
            "parent": {
                "command_line": {
                    "key": "process.command_line",
                    "object": "process_parent"
                },
                "entity_id": {
                    "key": "process.x_unique_id",
                    "object": "process_parent"
                },
                "name": [
                    {
                        "key": "process.name",
                        "object": "process_parent"
                    },
                    {
                        "key": "process.parent_ref",
                        "object": "process",
                        "references": "process_parent"
                    },
                ],
                "pid": [
                    {
                        "key": "process.pid",
                        "object": "process_parent",
                        "transformer": "ToInteger"
                    },
                    {
                        "key": "process.parent_ref",
                        "object": "process",
                        "references": "process_parent"
                    },
                ]
            },
            "pid": [
                {
                    "key": "process.pid",
                    "object": "process",
                    "transformer": "ToInteger"
                },
                {
                    "key": "x-oca-event.process_ref",
                    "object": "event",
                    "references": "process"
                }
            ],
            "name": [
                {
                    "key": "process.name",
                    "object": "process"
                },
                {
                    "key": "x-oca-event.process_ref",
                    "object": "event",
                    "references": "process"
                }
            ],
        },
        "protocol": {
            "key": "network-traffic.protocols",
            "object": "nt",
            "transformer": "ToLowercaseArray",
            "group": True
        },
        "hostname": [
            {
                "key": "x-oca-asset.hostname",
                "object": "host"
            }
        ],
        "application": {  # elastic_ecs has "nested" mappings
            "type": [
                {
                    "key": "network-traffic.protocols",
                    "object": "nt",
                    "group": "True",  # some maps use str "True" instead of bool true 
                    "transformer": "ToLowercaseArray"
                }
            ]
        },
        "user": {  # Map BOTH "name" and "id" to the same thing - what should happen?
            "name": [
                {
                    "key": "user-account.user_id",
                    "object": "user"
                },
                {
                    "key": "user-account.account_login",
                    "object": "user"
                }
            ],
            "id": [
                {
                    "key": "user-account.user_id",
                    "object": "user"
                }
            ]
        },
        "qid": [
            {
                "key": "x-custom-obj.qid",
                "object": "custom-obj",
                "transformer": "ToInteger"
            },
            {
                "key": "x-custom-obj.const",
                "object": "custom-obj",
                "value": 1
            }
        ]
    }
    transformers = {
        'ToInteger': lambda x: int(x),
        'ToLowercaseArray': ToLowercaseArray
    }

    # Fake up some data
    events = [
        {
            "foo": "bar",  # Unmapped column
            "timestamp": "1675275995001",
            "hostname": "ATLWKS138",
            "ip": ["192.168.1.1"],
            "mac": ["01:02:03:AA:BB:CC"],
            "sourceip": "192.168.1.1",
            "sourceport": 51275,
            "protocol": "TCP",
            "qid": "12345678"
        },
        {
            "foo": "bar",  # Unmapped column
            "timestamp": "1675275995002",
            "hostname": "ATLSRV1",
            "ip": ["10.0.0.1", "192.168.123.1"],
            "mac": ["40:50:60:DD:EE:FF", "01:02:03:DD:EE:FF"],
            "sourceip": "192.168.1.2",
            "sourceport": 51276,
            "protocol": "TCP",
            "application": {
                "type": "ssh"
            },
            "qid": "12345679"
        },
        {
            "timestamp": "1675275995003",
            "sourceip": "2001:db8:85a3:8d3:1319:8a2e:370:7348",
            "process": {
                "parent": {
                    "name": "services.exe",
                    "pid": 1048,
                    "entity_id": "{8dfc401c-d536-625b-0b00-000000002100}",
                    "executable": "C:\\Windows\\System32\\services.exe",
                    "command_line": "C:\\Windows\\system32\\services.exe"
                },
                "name": "svchost.exe",
                "working_directory": "C:\\Windows\\system32\\",
                "pid": 1992,
                "entity_id": "{8dfc401c-6275-627f-8035-000000002100}",
                "hash": {
                    "sha256": "cb19fd67b1d028e01f54c426a0924528c4a8d8ed8996cfe0ee0c6e45285436a1",
                    "md5": "1b280ad032268a636ecfe6f9165431b7"
                },
                "executable": "C:\\Windows\\System32\\svchost.exe",
                "command_line": "C:\\Windows\\system32\\svchost.exe -k LocalServiceNetworkRestricted -p -s WinHttpAutoProxySvc"
            },
            "user": {
                "id": 1001,
                "name": "paul"
            }
        },
    ]

    df = translate(stix_map, transformers, events, data_source)
    df = df.replace({np.NaN: None})
    print(sorted(list(df.columns)))

    # Test for duplicate columns
    assert len(df.columns) == len(set(list(df.columns)))
    print(json.dumps(df.to_dict(orient='records'), indent=4))
    assert len(df.index) == 3

    assert 'host_ipv6_0#ipv6-addr:value' not in df.columns

    col = 'host#x-oca-asset:ip_refs'
    assert col in df.columns
    assert df[col].iloc[0] == ['ipv4-addr--cd2ddd9b-6ae2-5d22-aec9-a9940505e5d5']
    assert df[col].iloc[1] == ['ipv4-addr--7dd44d27-f473-5ba9-b12b-0d3a61bbed2e', 'ipv4-addr--1037c297-4eb1-5505-9784-0303035746fc']
    assert df[col].iloc[2] is None

    col = 'host_mac_0#mac-addr:value'
    assert col in df.columns
    assert df[col].iloc[0] == '01:02:03:AA:BB:CC'
    assert df[col].iloc[1] == '40:50:60:DD:EE:FF'
    assert df[col].iloc[2] is None

    col = 'host_ip_0#ipv4-addr:value'
    assert col in df.columns
    assert df[col].iloc[0] == '192.168.1.1'
    assert df[col].iloc[1] == '10.0.0.1'
    assert df[col].iloc[2] is None

    col = 'host_ip_1#ipv4-addr:value'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] == '192.168.123.1'
    assert df[col].iloc[2] is None

    col = 'src_ip#ipv6-addr:value'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == "2001:db8:85a3:8d3:1319:8a2e:370:7348"

    col = 'src_ip#ipv4-addr:value'
    assert col in df.columns
    assert df[col].iloc[0] == '192.168.1.1'
    assert df[col].iloc[1] == '192.168.1.2'
    assert df[col].iloc[2] is None

    col = 'nt#network-traffic:src_ref'
    assert col in df.columns
    assert df[col].iloc[0] == 'ipv4-addr--cd2ddd9b-6ae2-5d22-aec9-a9940505e5d5'
    assert df[col].iloc[1] == 'ipv4-addr--1c7d5746-e728-5d1c-bb2b-deb4020f547f'
    assert df[col].iloc[2] == 'ipv6-addr--985b9abb-05e9-522b-a869-f7db86c19a2b'

    col = 'nt#network-traffic:src_port'
    assert col in df.columns
    assert df[col].iloc[0] == 51275
    assert df[col].iloc[1] == 51276
    assert pd.isna(df[col].iloc[2])

    col = 'nt#network-traffic:protocols'
    assert col in df.columns
    assert df[col].iloc[0] == ['tcp']
    assert df[col].iloc[1] == ['tcp', 'ssh']
    #FIXME:assert df[col].iloc[2] is None

    col = 'custom-obj#x-custom-obj:qid'
    assert col in df.columns
    assert df[col].iloc[0] == 12345678
    assert df[col].iloc[1] == 12345679
    assert pd.isna(df[col].iloc[2])

    col = 'custom-obj#x-custom-obj:const'
    assert col in df.columns
    assert df[col].iloc[0] == 1
    assert df[col].iloc[1] == 1
    assert df[col].iloc[2] == 1

    fo_col = 'observed-data:first_observed'
    assert fo_col in df.columns
    lo_col = 'observed-data:last_observed'
    assert lo_col in df.columns
    assert df[fo_col].iloc[0] == '2023-02-01T18:26:35.001000Z'
    assert df[lo_col].iloc[0] == '2023-02-01T18:26:35.001000Z'
    assert df[fo_col].iloc[1] == '2023-02-01T18:26:35.002000Z'
    assert df[lo_col].iloc[1] == '2023-02-01T18:26:35.002000Z'
    assert df[fo_col].iloc[2] == '2023-02-01T18:26:35.003000Z'
    assert df[lo_col].iloc[2] == '2023-02-01T18:26:35.003000Z'

    col = 'process#process:name'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == 'svchost.exe'

    col = 'process#process:parent_ref'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == df['process_parent#process:id'].iloc[2]

    col = 'process_parent#process:name'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == 'services.exe'

    col = 'user#user-account:user_id'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == 1001

    col = 'user#user-account:account_login'
    assert col in df.columns
    assert df[col].iloc[0] is None
    assert df[col].iloc[1] is None
    assert df[col].iloc[2] == 'paul'


def test_unmapped_col():
    # Example STIX mapping
    stix_map = {
        "timestamp": [
            {
                "key": "first_observed",
                "transformer": "EpochToTimestamp",
                "cybox": False
            },
            {
                "key": "last_observed",
                "transformer": "EpochToTimestamp",
                "cybox": False
            },
        ],
        "foo": {
            "bar": {
                "key": "x-foo-object.bar"
            }
        }
    }
    transformers = {}

    # Fake up some data
    # Note there's a mapping for foo.bar, but not just plain old "foo"
    events = [
        {
            "foo": "bar",  # "Half-unmapped" column
            "timestamp": "1675275995001",
        }
    ]
    df = translate(stix_map, transformers, events, data_source)

    events = [
        {
            "foo": {
                "baz": "wowee"
            },
            "timestamp": "1675275995002",
        },
        {
            "timestamp": "1675275995003",
        },
    ]
    df = translate(stix_map, transformers, events, data_source)


@pytest.mark.asyncio
async def test_ingest(tmpdir):
    df = pd.DataFrame(
        {
            "src_ip#ipv4-addr:value": ['192.168.1.1', '192.168.1.2', None],
            "src_ip#ipv6-addr:value": [None, None, "2001:db8:85a3:8d3:1319:8a2e:370:7348"],
            "x-custom-obj#x-custom-obj:qid": [12345678, 12345679, None]
        })

    store = await async_storage(tmpdir)
    await ingest(store, data_source, df, 'my-query-id')
    await store.delete()
