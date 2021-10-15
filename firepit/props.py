"""Utility functions for STIX properties"""

import re


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
                      'software', 'windows-registry-value-type']:
        prop = 'name'
    elif sco_type == 'directory':
        prop = 'path'
    elif sco_type == 'autonomous-system':
        prop = 'number'
    elif sco_type == 'windows-registry-key':
        prop = 'key'
    elif sco_type == 'x509-certificate':
        prop = 'serial_number'
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
    if part == 'parent_ref':
        return ['process']
    elif part in ['dst_ref', 'dst_ip_ref', 'src_ref', 'src_ip_ref']:
        return ['ipv4-addr', 'ipv6-addr']
    elif sco_type in ['ipv4-addr', 'ipv6-addr'] and part == 'resolves_to_refs':
        return ['mac-addr']
    elif part == 'binary_ref':
        return ['file']
    elif part == 'parent_directory_ref':
        return ['directory']
    elif part == 'creator_user_ref':
        return ['user-account']
    elif part in ['dst_os_ref', 'src_os_ref',
                  'dst_application_ref', 'src_application_ref']:  # x-ibm-finding
        return ['software']
    elif part == ['ip_refs']:  # x-oca-asset, x-oca-event, x-oca-pod-ext
        return ['ipv4-addr', 'ipv6-addr']
    elif part == ['mac_refs']:  # x-oca-asset
        return ['mac-addr']
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

    # TODO: hueristics/classifier to guess?
    raise NotImplementedError(f'{sco_type}:{part}')  # TEMP


def is_ref(name):
    return name.endswith('_ref') \
        or name.endswith('_refs')


def parse_path(path):
    sco_type, _, prop = path.rpartition(':')
    parts = prop.split('.')
    result = []
    prev_type = sco_type
    for part in parts:
        if not is_ref(part):
            result.append(('node', prev_type, part))
            prev_type = part
        else:
            cur_type = sco_type
            sco_type = ref_type(cur_type, part)
            if isinstance(sco_type, list):
                sco_type = sco_type[0]  # FIXME: How should we handle lists?
            result.append(('rel', cur_type, part, sco_type))
            prev_type = sco_type
    return result
