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
    'process': tuple(),  # !!!
    'software': ('name', 'cpe', 'swid', 'vendor', 'version'),
    'url': ('value',),
    'user-account': ('account_type', 'user_id', 'account_login'),
    'windows-registry-keys': ('key', 'values'),
    'x509-certificate': ('hashes', 'serial_number'),
}


def makeid(sco):
    sco_type = sco['type']
    contrib = {}  # the ID contributing properties
    props = ID_PROPS.get(sco_type)
    if props:
        num = len(props)

        # hashes is a special case.  Choose first hash according to spec.
        hashes = sco.get('hashes')
        if props[0] == 'hashes' and hashes:
            for hash_type in HASHES_PREF_LIST:
                value = hashes.get(hash_type)
                if value:
                    contrib[f'hashes'] = {hash_type.strip("'"): value}
                    break
            else:
                # None of the preferred hashes found
                prop = sorted(list(hashes.keys()))[0]
                contrib[f'hashes'] = {prop.strip("'"): hashes[prop]}
        elif props[0] in sco:
            contrib[props[0]] = sco[props[0]]

        for i in range(1, num):
            if props[i] in sco:
                contrib[props[i]] = sco[props[i]]

    elif sco_type == 'process':
        # Special non-STIX 2.1 case for process?
        # Could include name, pid, created_by_ref (might help), some timestamp?
        # CbR: x_unique_id
        # Others?
        # Use different namespace?
        pass
        
    if contrib:
        name = ujson.dumps(contrib, sort_keys=True, ensure_ascii=False)
        oid = f'{sco_type}--{str(uuid.uuid5(ID_NAMESPACE, name))}'
    else:
        oid = f'{sco_type}--{str(uuid.uuid4())}'

    return oid
