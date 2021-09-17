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
