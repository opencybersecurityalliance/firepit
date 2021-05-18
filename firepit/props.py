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

    # Don't aggregate certain columns; ignore them
    last = get_last(prop)
    if last in ['x_root', 'x_contained_by_ref', 'type', 'id']:
        return None

    if prop == 'number_observed':
        return 'SUM("number_observed") AS "number_observed"'
    elif prop in ['first_observed', 'start']:
        return f'MIN("{prop}") AS "{prop}"'
    elif prop in ['last_observed', 'end']:
        return f'MAX("{prop}") AS "{prop}"'

    if ((sco_type == 'network-traffic' and prop.endswith('_port'))
        or (sco_type == 'process' and prop.endswith('pid'))):
        agg = f'COUNT(DISTINCT "{prop}")'
        alias = f'"unique_{prop}"'
    elif col_type.lower() in ['integer', 'bigint']:
        agg = f'AVG("{prop}")'
        alias = f'"mean_{prop}"'
    else:
        agg = f'COUNT(DISTINCT "{prop}")'
        alias = f'"unique_{prop}"'

    if len(alias) > 63:
        # PostgreSQL has a limit of 63 chars per identifier
        return None

    return f'{agg} AS {alias}'
