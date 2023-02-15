# Reference: https://docs.oasis-open.org/cti/stix/v2.1/os/stix-v2.1-os.html

import uuid

import ujson


ID_NAMESPACE = uuid.UUID('00abedb4-aa42-466c-9c01-fed23315a9b7')

HASHES_PREF_LIST = ["MD5", "'SHA-1'", "'SHA-256'", "'SHA-512'"]

ID_PROPS = {
    'artifact': ('hashes', 'payload_bin'),
    'autonomous-system': ('number',),
    'directory': ('path',),
    'domain-name': ('value',),
    'email-addr': ('value',),
    'email-message': ('from_ref', 'subject', 'body'),
    'file': ('hashes', 'name', 'extensions', 'parent_directory_ref'),
    'ipv4-addr': ('value',),
    'ipv6-addr': ('value',),
    'mac-addr': ('value',),
    'mutex': ('name',),
    'network-traffic': ('start', 'end', 'src_ref', 'dst_ref', 'src_port', 'dst_port', 'protocols', 'extensions'),
    'process': ('x_unique_id',),  # This is non-standard, obviously
    'software': ('name', 'cpe', 'swid', 'vendor', 'version'),
    'url': ('value',),
    'user-account': ('account_type', 'user_id', 'account_login'),
    'windows-registry-keys': ('key', 'values'),
    'x509-certificate': ('hashes', 'serial_number'),

    # Common extensions
    'x-oca-asset': ('host_id', 'device_id', 'name', 'hostname'),
}

PROCESS_UNIQUE_ID_PROPS = [
    'process_id', # reaqta (older version)
    'process_uid', # reaqta (newer)
    'process_unique_id', # sentinelone
    'process_guid', # just in case
]


def _get_asset_id(obs):
    """Find the first x-oca-asset and return ID or hostname"""
    for _, sco in obs.get('objects', {}).items():
        if sco['type'] == 'x-oca-asset':
            for prop in ('host_id', 'hostname'):
                if prop in sco:
                    return sco[prop]


def makeid(sco, obs=None):
    sco_type = sco['type']
    contrib = {}  # the ID contributing properties
    props = ID_PROPS.get(sco_type, [])
    for prop in props:
        if prop == 'hashes':
            # hashes is a special case.  Choose first hash according to spec.
            hashes = sco.get('hashes')
            if hashes:
                for hash_type in HASHES_PREF_LIST:
                    value = hashes.get(hash_type)
                    if value:
                        contrib['hashes'] = {hash_type.strip("'"): value}
                        break
                else:
                    # None of the preferred hashes found
                    prop = sorted(list(hashes.keys()))[0]
                    contrib['hashes'] = {prop.strip("'"): hashes[prop]}
        elif prop in sco:
            value = sco[prop]
            if prop.endswith('_ref') and obs:  # Hook for STIX 2.0 SCOs
                target = obs['objects'].get(value)
                if target:
                    value = makeid(target)
                    contrib[prop] = value
            else:
                contrib[prop] = value

    if sco_type == 'process' and 'x_unique_id' not in contrib:
        unique_id = None
        for _, ext in sco.get('extensions', {}).items():
            for prop in PROCESS_UNIQUE_ID_PROPS:
                unique_id = ext.get(prop)
                if unique_id:
                    contrib['x_unique_id'] = unique_id
                    break
            if unique_id:
                break
        else:  # Still don't have unique_id
            if obs:
                # Try to use other SCOs
                pid = sco.get('pid')
                asset = _get_asset_id(obs)
                if pid and asset and obs:
                    ts = obs['last_observed']
                    contrib['x_unique_id'] = f'{pid}_{asset}_{ts}'

    if contrib:
        name = ujson.dumps(contrib, sort_keys=True, ensure_ascii=False)
        oid = f'{sco_type}--{str(uuid.uuid5(ID_NAMESPACE, name))}'
    else:
        oid = f'{sco_type}--{str(uuid.uuid4())}'

    return oid
