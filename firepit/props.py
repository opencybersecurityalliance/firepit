"""Utility functions for STIX properties"""

import re


# This is a subset of known STIX objects and properties.
# Ideally we would "learn" all this while ingesting data
#
# dtype: Python data type
# ftype: "feature" type, as in ML feature
#        one of timestamp, numerical, or categorical (really "other")
#
# Maybe we only need entries if dtype IS NOT str?
# dtype == 'str' -> 'ftype' == 'categorical'
KNOWN_PROPS = {
    'artifact': {
        'payload_bin': {
            'dtype': 'str',
        },
    },
    'autonomous-system': {
        'number': {
            'dtype': 'int',
            'ftype': 'categorical',
        },
        'name': {
            'dtype': 'str',
        },
        'rir': {
            'dtype': 'str',
        },
    },
    'directory': {
        'accessed': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'created': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'modified': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'atime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'ctime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'mtime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'path': {
            'dtype': 'str',
        },
    },
    'domain-name': {
        'value': {
            'dtype': 'str',
        },
    },
    'email-addr': {
        'value': {
            'dtype': 'str',
        },
    },
    'email-message': {
        'is_multipart': {
            'dtype': 'bool',
        },
        'date': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'message_id': {
            'dtype': 'str',
        },
    },
    'file': {
        'accessed': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'created': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'modified': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'atime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'ctime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'mtime': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'name': {
            'dtype': 'str',
        },
        #TODO? 'hashes': {
    },
    'ipv4-addr': {
        'value': {
            'dtype': 'str',
        },
    },
    'ipv6-addr': {
        'value': {
            'dtype': 'str',
        },
    },
    'mac-addr': {
        'value': {
            'dtype': 'str',
        },
    },
    'mutex': {
        'value': {
            'dtype': 'str',
        },
    },
    'network-traffic': {
        'protocols': {
            'dtype': 'list',
        },
        'dst_port': {
            'dtype': 'int',
            'ftype': 'categorical',
        },
        'src_port': {
            'dtype': 'int',
            'ftype': 'categorical',
        },
        'dst_byte_count': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'src_byte_count': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'src_packets': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'dst_packets': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'ipfix.flowId': {  # Standard extension-like
            'dtype': 'str',
        },
        'ipfix.maximumIpTotalLength': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'ipfix.minimumIpTotalLength': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'start': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'end': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
    },
    'process': {
        'created': {  # STIX 2.0
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'created_time': {  # STIX 2.1
            'dtype': 'str',
            'ftype': 'timestamp',
        },
    },
    'software': {
    },
    'url': {
        'value': {
            'dtype': 'str',
        },
    },
    'user-account': {
        'user_id': {
            'dtype': 'str',
        },
        'account_login': {
            'dtype': 'str',
        },
        'account_created': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'account_expires': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'credential_last_changed': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'account_first_login': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'account_last_login': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
    },
    'windows-registry-key': {
        'modified': {  # STIX 2.0
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'modified_time': {  # STIX 2.1
            'dtype': 'str',
            'ftype': 'timestamp',
        },
    },
    'x509-certificate': {
        'validity_not_after': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'validity_not_before': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
    },
    'x-ibm-finding': {
        'time_observed': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'start': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'end': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'rule_trigger_count': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'severity': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
        'event_count': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
    },
    'x-oca-asset': {
    },
    'x-oca-event': {
        'created': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'start': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'end': {
            'dtype': 'str',
            'ftype': 'timestamp',
        },
        'code': {
            'dtype': 'int',
            'ftype': 'categorical',
        },
        'duration': {
            'dtype': 'int',
            'ftype': 'numerical',
        },
    },

    # SDOs
    'observed-data': {
        'first_observed': {
            'dtype': 'str',
            'ftype': 'timestamp'
        },
        'last_observed': {
            'dtype': 'str',
            'ftype': 'timestamp'
        },
        'number_observed': {
            'dtype': 'int',
            'ftype': 'numerical'
        },
    },
}


LIKELY_TIMESTAMPS = {
    prop
    for sco_type, props in KNOWN_PROPS.items()
    for prop, metadata in props.items()
    if metadata.get("ftype") == "timestamp"
}


def path_metadata(path):
    """Get metadata for a STIX object path"""
    sco_type, _, prop = path.rpartition(':')
    return prop_metadata(sco_type, prop)


def prop_metadata(sco_type, prop):
    """Get metadata for a STIX object property"""
    meta = KNOWN_PROPS.get(sco_type, {}).get(prop)
    if not meta:
        links = parse_prop(sco_type, prop)  # Maybe just do this first?
        if links:
            _, ref_type, ref_prop = links[-1]
            meta = KNOWN_PROPS.get(ref_type, {}).get(ref_prop, {})
        else:
            meta = {}
    if 'dtype' not in meta:
        meta['dtype'] = 'str'
    if 'ftype' not in meta:
        # Heuristic based on name
        if (prop.endswith('time') or prop.startswith('time') or
            prop in LIKELY_TIMESTAMPS):
            meta['ftype'] = 'timestamp'
        elif prop.endswith('count') or prop.startswith('count'):
            meta['ftype'] = 'numerical'
        else:
            meta['ftype'] = 'categorical'
    return meta


# A regex to grab the last piece of a STIX path
last_re = re.compile(r'.*[\.:]([a-z]*)')


def get_last(prop):
    return last_re.sub(r'\g<1>', prop)


def primary_prop(sco_type):
    """Returns the "primary" property name for each SCO type"""
    prop = 'value'  # Generic catchall
    if sco_type == 'user-account':
        prop = 'user_id'
    elif sco_type in ['file', 'mutex', 'process',
                      'software', 'windows-registry-value-type',
                      'x-ibm-finding']:
        prop = 'name'
    elif sco_type == 'directory':
        prop = 'path'
    elif sco_type == 'autonomous-system':
        prop = 'number'
    elif sco_type == 'windows-registry-key':
        prop = 'key'
    elif sco_type == 'x509-certificate':
        prop = 'serial_number'
    elif sco_type == 'x-oca-asset':
        prop = 'hostname'
    elif sco_type == 'x-oca-event':
        prop = 'action'
    return prop


def auto_agg(sco_type, prop, col_type):
    """Infer an aggregation function based on column name and type"""

    agg = auto_agg_tuple(sco_type, prop, col_type)
    if not agg:
        return None

    func, col, alias = agg

    if len(alias) > 63:
        # PostgreSQL has a limit of 63 chars per identifier
        return None

    # Special case for NUNIQUE (which is not SQL)
    if func == 'NUNIQUE':
        return f'COUNT(DISTINCT "{col}") AS "{alias}"'
    return f'{func}("{col}") AS "{alias}"'


def auto_agg_tuple(sco_type, prop, col_type):
    """Infer an aggregation function based on column name and type"""
    # Don't aggregate certain columns; ignore them
    last = get_last(prop)
    if last in ['x_root', 'x_contained_by_ref', 'type', 'id']:
        return None

    #if last.endswith('_observed'):  # TEMP
    #    return None

    if prop == 'number_observed':
        return ('SUM', prop, prop)
    elif prop in ['first_observed', 'start']:
        return ('MIN', prop, prop)
    elif prop in ['last_observed', 'end']:
        return ('MAX', prop, prop)

    if ((sco_type == 'network-traffic' and prop.endswith('_port'))
        or (sco_type == 'process' and prop.endswith('pid'))):
        func = 'NUNIQUE'
        alias = f'unique_{prop}'
    elif col_type.lower() in ['integer', 'bigint']:
        func = 'AVG'
        alias = f'mean_{prop}'
    else:
        func = 'NUNIQUE'
        alias = f'unique_{prop}'

    return (func, prop, alias)


#TODO: convert to dicts?
def ref_type(sco_type, part):
    """Get STIX SCO type for reference prop `part`"""
    if part == 'parent_ref':
        return ['process']
    elif part in ['dst_ref', 'dst_ip_ref', 'src_ref', 'src_ip_ref']:
        return ['ipv4-addr', 'ipv6-addr']
    elif sco_type in ['ipv4-addr', 'ipv6-addr'] and part == 'resolves_to_refs':
        return ['mac-addr']
    elif part in ['binary_ref', 'image_ref']:
        return ['file']
    elif part == 'parent_directory_ref':
        return ['directory']
    elif part == 'creator_user_ref':
        return ['user-account']
    elif part in ['dst_os_ref', 'src_os_ref',
                  'dst_application_ref', 'src_application_ref']:  # x-ibm-finding
        return ['software']
    elif part == 'ip_refs':  # x-oca-asset, x-oca-event, x-oca-pod-ext
        return ['ipv4-addr', 'ipv6-addr']
    elif part == 'mac_refs':  # x-oca-asset
        return ['mac-addr']
    elif part == 'opened_connection_refs':
        return ['network-traffic']
    elif part in ['src_payload_ref', 'dst_payload_ref']:
        return ['artifact']
    elif sco_type == 'x-oca-event':
        if part == 'original_ref':
            return ['artifact']
        elif part == 'host_ref':
            return ['x-oca-asset']
        elif part == 'url_ref':
            return ['url']
        elif part == 'file_ref':
            return ['file']
        elif 'process' in part:
            return ['process']
        elif part == 'domain_ref':
            return ['domain-name']
        elif part == 'registry_ref':
            return ['windows-registry-key']
        elif part == 'network_ref':
            return ['network-traffic']
        elif part == 'user_ref':
            return ['user-account']
    elif sco_type == 'x-ibm-finding':
        if part.endswith('_user_ref'):
            return ['user-account']
    elif (sco_type == 'email-message' and
          part in ['from_ref', 'sender_ref', 'to_refs', 'cc_refs', 'bcc_refs']):
        return ['email-addr']

    # TODO: hueristics/classifier to guess?
    #raise NotImplementedError(f'{sco_type}:{part}')  # TEMP
    return []


def is_ref(name):
    return name.endswith('_ref') \
        or name.endswith('_refs')


def parse_path(path):
    sco_type, _, prop = path.rpartition(':')
    return parse_prop(sco_type, prop)


def parse_prop(sco_type, prop):
    if '_ref.' not in prop and '_refs' not in prop:
        return [('node', sco_type, prop)]
    parts = prop.split('.')
    result = []
    prev_type = sco_type
    for part in parts:
        if part.endswith('[*]'):
            is_list = True
            part = part[:-3]
        else:
            is_list = False
        if not is_ref(part):
            if is_list:
                part += '[*]'
            result.append(('node', prev_type, part))
            prev_type = part
        else:
            cur_type = sco_type
            sco_type = ref_type(cur_type, part)
            if isinstance(sco_type, list):
                if len(sco_type) == 0:
                    # We don't know what this ref could point to!
                    return []
                sco_type = sco_type[0]  # FIXME: How should we handle lists?
            result.append(('rel', cur_type, part, sco_type))
            prev_type = sco_type
    return result
