from .helpers import tmp_storage

def test_null_clobber(tmpdir):
    store = tmp_storage(tmpdir)

    # First bundle has x_extra value
    b1 = {
        "type": "bundle",
        "id": "bundle--0911b0a3-7a32-4bd5-bddd-5757bd87e8a0",
        "spec_version": "2.0",
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
                "type": "observed-data",
                "id": "observed-data--4bd9c203-a327-4b81-b2fa-e6fc8d705dcc",
                "created_by_ref": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.651Z",
                "modified": "2020-06-30T19:31:23.651Z",
                "first_observed": "2020-06-30T19:25:09.447726Z",
                "last_observed": "2020-06-30T19:28:49.692424Z",
                "number_observed": 1,
                "objects": {
                    "0": {
                        "type": "ipv4-addr",
                        "value": "192.168.212.97",
                        "x_extra": "foo"
                    }
                }
            }
        ]
    }
    store.cache('b1', b1)

    # Second bundle does not have x_extra value
    b2 = {
        "type": "bundle",
        "id": "bundle--123d417e-e745-4017-8b2c-b3f710b91457",
        "spec_version": "2.0",
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
                "type": "observed-data",
                "id": "observed-data--bd5309ae-149b-4bb1-80a6-331e3ef82ee2",
                "created_by_ref": "identity--ec1709c3-63a6-4fac-94d7-e648355d35a4",
                "created": "2020-06-30T19:31:23.483Z",
                "modified": "2020-06-30T19:31:23.483Z",
                "first_observed": "2020-06-30T19:29:56.900714Z",
                "last_observed": "2020-06-30T19:29:56.931378Z",
                "number_observed": 1,
                "objects": {
                    "0": {
                        "type": "ipv4-addr",
                        "value": "192.168.212.97",
                    }
                }
            }
        ]
    }
    store.cache('b2', b2)

    # Ensure that the missing value didn't clobber the original
    values = store.values('ipv4-addr:x_extra', 'ipv4-addr')
    assert values[0] == "foo"
