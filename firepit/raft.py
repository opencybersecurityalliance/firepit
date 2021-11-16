#!/usr/bin/env python

"""
Raft: Streaming processing of STIX Observation SDOs (`observed-data`).
"""

from collections import OrderedDict
from collections import defaultdict

import ijson
import orjson
import ujson
import requests

from firepit import stix21


INVERTED_REFS = {
    'x_child_of_ref': 'parent_ref',
    'x_resolves_to_of_ref': 'x_resolves_from_ref',
    'x_encapsulates_of_ref': 'encapsulated_by_ref',
}


class GeneratorIO:
    '''Convert a generator into a file-like object'''

    def __init__(self, gen):
        self.gen = gen
        self.buf = b''

    def read(self, n):
        result = b''
        try:
            while n > len(self.buf):
                self.buf += next(self.gen)
            result = self.buf[:n]
            self.buf = self.buf[n:]
        except StopIteration:
            result = self.buf
            self.buf = b''
        return result


def _get_objects(fp, types):
    try:
        for obj in ijson.items(fp, 'objects.item'):
            if not types or obj['type'] in types:
                yield obj
    except ijson.common.IncompleteJSONError:
        pass


def _yield_objects(bundle, types):
    if 'type' not in bundle or bundle['type'] != 'bundle':
        bundle = {}
    for obj in bundle.get('objects', []):
        if not types or obj.get('type') in types:
            yield obj


def get_objects(source, types=None):
    '''A generator function that yields STIX objects from source'''
    if isinstance(source, dict):
        for obj in source.get('objects', []):
            yield obj
    elif source.startswith('http'):
        response = requests.get(source, stream=True)
        fp = GeneratorIO(response.iter_content(chunk_size=8192))
        yield from _get_objects(fp, types)
    elif hasattr(source, 'read'):
        yield from _get_objects(source, types)
    else:
        with open(source, 'r') as fp:
            bundle = orjson.loads(fp.read())
        yield from _yield_objects(bundle, types)


def _is_custom_obj(obj):
    return obj['type'].startswith('x-')


def _is_custom_prop(prop):
    return prop.startswith('x_')


def _set_id(obs):
    sid = stix21.makeid(obs)
    obs['id'] = sid
    return sid


def preserve(obj):
    '''Stash the "raw" STIX JSON as a stirng attribute in the object itself'''
    if obj['type'] == 'observed-data':
        obj['x_stix'] = ujson.dumps(obj)
    return obj


def promote(obj):
    '''Given an Observation, "promote" contained SCOs to top level'''
    # TODO: also "promote"/synthesize SROs for references?
    observables = obj['objects']
    oid = obj['id']
    object_refs = []
    results = []
    if isinstance(observables, dict):
        for idx, obs in observables.items():
            obs['x_contained_by_ref'] = oid
            for prop in ['first_observed', 'last_observed', 'number_observed']:
                if prop in obj:
                    obs[prop] = obj[prop]
            uid = _set_id(obs)
            for prop, val in obs.items():
                if prop.endswith('_ref') or prop.endswith('_refs'):
                    if not isinstance(val, list):
                        val = [val]
                    for ref in val:
                        if ref in observables and ref != idx:  # Avoid bogus references
                            target = observables[ref]
                            obs[prop] = f'{target["type"]}--{uid}_{ref}'
            object_refs.append(obs['id'])
            results.append(obs)
    elif isinstance(observables, list):
        # Already ran through nest?
        for idx, orig_obs in enumerate(observables):
            obs = orig_obs.copy()
            obs['x_contained_by_ref'] = oid
            for prop in ['first_observed', 'last_observed', 'number_observed']:
                if prop in obj:
                    obs[prop] = obj[prop]
            object_refs.append(obs['id'])
            results.append(obs)
    del obj['objects']
    results.append(obj)
    return results


def makeid(obj):
    '''
    Deprecated: moved into `markroot`
    Add unique object IDs to SCOs inside an Observation SDO
    '''
    observables = obj.get('objects', {})
    for idx, obs in observables.items():
        _set_id(obs)
    return obj


def _resolve(obs_orig, observables):
    obs = {}
    is_custom_obj = _is_custom_obj(obs_orig)  #obs_orig['type'].startswith('x-')
    for prop, val in obs_orig.items():
        if ((prop.endswith('_ref') or prop.endswith('_refs')) and
            prop != 'child_refs'):
            id_only = False
            if is_custom_obj and not _is_custom_prop(prop):  # prop.startswith('x_'):
                id_only = True
            if not isinstance(val, list):
                if val in observables:
                    target = observables[val]
                    if id_only:
                        obs[prop] = {'id': target['id']}
                    elif not _is_custom_obj(target):
                        target = _resolve(target, observables)
                        obs[prop] = target
            else:
                obs[f'{prop}_count'] = len(val)
                for i, ref in enumerate(val):
                    if ref in observables:  # Avoid bogus references
                        target = observables[ref]
                        target = _resolve(target, observables)
                        obs[f'{prop}[{i}]'] = target
        else:
            obs[prop] = val
    return obs


# TODO: rename to "dereference"?  "resolve"?
def nest(obj):
    '''Resolve refs with deep copy'''
    object_refs = []
    observables = obj.get('objects', {})
    for _, obs_orig in observables.items():
        object_refs.append(_resolve(obs_orig, observables))
    del obj['objects']
    obj['objects'] = object_refs
    return obj


def json_normalize(d, prefix='', sep='.', flat_lists=True):
    r = OrderedDict()  # {}
    for k, v in d.items():
        if '-' in k:  # Weird STIX rule: single quotes around things like SHA-1
            if ':' in k:
                otype, _, path = k.rpartition(':')
                parts = path.split('.')
                key = f"{otype}:" + '.'.join([f"'{part}'" if '-' in part else part for part in parts])
            else:
                key = f"'{k}'"
        else:
            key = k
        if prefix:
            key = f'{prefix}{sep}{key}'
        if isinstance(v, dict):
            r.update(json_normalize(v, key, sep, flat_lists))
        elif flat_lists and isinstance(v, list):
            for i, val in enumerate(v):
                r[f'{key}[{i}]'] = val
        else:
            r[key] = v
    return r


def normalize(obj):
    """Normalize obj to a flat dict"""
    return json_normalize(obj, flat_lists=False)


def invert(obj):
    '''Invert reference lists so all refs are 1:1'''
    objects = obj['objects']

    for k, v in objects.items():
        reflists = []
        for attr, val in v.items():
            if attr.endswith('_refs') and isinstance(val, list):
                refname = 'x_' + attr.rstrip('_refs') + '_of_ref'
                refname = INVERTED_REFS.get(refname, refname)
                for ref in val:
                    if ref not in objects or ref == k:  # Detect bogus refs
                        continue
                    target = objects[ref]
                    target[refname] = k
                reflists.append(attr)

        for attr in reflists:
            # Record the count of the ref list we've inverted
            v['x_' + attr.rstrip('_refs') + '_count'] = len(v[attr])
            del v[attr]

    return obj


def upgrade_2021(obs):
    """
    Upgrade a 2.0 observation to a 2.1 observation
    """
    results = [obs]
    if 'objects' not in obs:
        return results
    scos = obs['objects']
    object_refs = set()
    ref_map = {}
    for idx, sco in scos.items():
        # Assign a STIX 2.1-style identifier
        sid = _set_id(sco)
        ref_map[idx] = sid
        object_refs.add(sid)
        sco['spec_version'] = '2.1'
        results.append(sco)

    # Resolve 2.0-style refs to new style
    for obj in results:  # Includes SDOs, SCOs, and SROs
        if obj['type'] == 'relationship':
            continue

        for prop, val in obj.items():
            if prop.endswith('_ref'):
                if val.isdigit():
                    obj[prop] = ref_map[val]
            elif prop.endswith('_refs'):
                refs = []
                if isinstance(val, list):
                    for i in val:
                        if i.isdigit():
                            refs.append(ref_map[i])
                else:
                    if val.isdigit():
                        refs.append(ref_map[val])
                if refs:
                    obj[prop] = refs
                else:
                    del obj[prop]

    del obs['objects']
    obs['object_refs'] = list(object_refs)
    obs['spec_version'] = '2.1'

    return results


def _rank(results, sco_id, rank):
    """Set rank on __contains relationship for SCO"""
    for result in results:
        if result['type'] == '__contains' and result['target_ref'] == sco_id:
            result['x_firepit_rank'] = 1



def make_sro_21(obj):  # TODO: better name
    """
    For STIX 2.1 objects, convert ref lists to SROs
    """
    results = [obj]
    oid = obj['id']

    obj_type = obj['type']
    if obj_type == 'identity':
        return results
    elif obj_type == 'observed-data':
        for ref in obj['object_refs']:
            # Append pseudo-relationship for "Observtion CONTAINS SCO"
            results.append({
                'type': '__contains',
                'source_ref': oid,
                'target_ref': ref
            })
        del obj['object_refs']
        return results

    # Create SRO for ref lists
    ref_lists = []
    for prop, val in obj.items():
        if prop.endswith('_refs'):
            if not isinstance(val, list):
                val = [val]
            if prop != 'object_refs':
                for ref in val:
                    if ref != oid:  # Avoid bogus references
                        sro = {
                            'type': '__reflist',
                            'ref_name': prop,
                            'source_ref': oid,
                            'target_ref': ref
                        }
                        results.append(sro)

            # Store prop name to remove later
            ref_lists.append(prop)

    for prop in ref_lists:
        del obj[prop]

    return results


def make_sro(obs):  # TODO: better name
    """
    Convert ref lists to SROs, add ids if missing, etc.
    """
    if obs.get('spec_version', '2.0') == '2.1':
        return make_sro_21(obs)

    scos = obs['objects']
    ref_map = {}
    results = [obs]

    # Keep track of the preference order of each reffed object, by type
    prefs = defaultdict(list)
    reffed = set()

    for idx, sco in scos.items():
        # Put SCO at end of pref list
        prefs[sco['type']].append(idx)

        # Assign a STIX 2.1-style identifier
        sid = stix21.makeid(sco, obs)
        sco['id'] = sid
        ref_map[idx] = sid

        # Create SRO for ref lists
        ref_lists = []
        for prop, val in sco.items():
            if prop.endswith('_ref'):
                # markroot stuff
                if val in scos and val != idx:  # Avoid bogus references
                    # If an object refs another object of the same type,
                    # only mark the root (think process:parent_ref)
                    if scos[idx]['type'] == scos[val]['type']:
                        _mark_tree(scos, val, reffed)
                    elif (scos[val]['type'].endswith('-addr')):
                        if 'dst_' in prop:
                            # For src/dst pairs, consider the src as the root (so add dst to reffed)
                            reffed.add(val)
                        elif prop.endswith('src_ref'):
                            # Save ref as the "preferred" object for this type
                            prefs[scos[val]['type']].insert(0, val)
                    elif val in reffed:
                        reffed.add(idx)
            elif prop.endswith('_refs'):
                if not isinstance(val, list):
                    val = [val]
                for ref in val:
                    if ref in scos and ref != idx:  # Avoid bogus references
                        # We'll replace these indices later
                        sro = {
                            'type': '__reflist',
                            'ref_name': prop,
                            'source_ref': idx,
                            'target_ref': ref
                        }
                        results.append(sro)

                        # markroot stuff
                        if scos[idx]['type'] == scos[ref]['type']:
                            reffed.add(ref)

                # Store prop name to remove later
                ref_lists.append(prop)

        for prop in ref_lists:
            del sco[prop]

        # Append pseudo-relationship for "Observtion CONTAINS SCO"
        results.append({
            'type': '__contains',
            'source_ref': obs['id'],
            'target_ref': sco['id']
        })

        # calc distance?

        #TODO: if sco in results already, update?
        results.append(sco)

    # Resolve 2.0-style refs to new style
    for obj in results:  # Includes SDOs, SCOs, and SROs
        if obj['type'] == 'relationship':
            continue

        for prop, val in obj.items():
            if prop.endswith('_ref'):
                ref = obj[prop]
                obj[prop] = ref_map.get(val, ref)  #FIXME: if ref not in map?

        obj_type = obj['type']
        k = None
        for idx, sid in ref_map.items():
            if sid == obj.get('id'):
                k = idx
        if k and k not in reffed:
            # Check if there's a more preferred object
            if obj_type not in prefs:
                _rank(results, scos[k]['id'], 1)
            else:
                for i in prefs[obj_type]:
                    if i in reffed:
                        continue
                    elif i == k:
                        _rank(results, scos[k]['id'], 1)
                    break

    del obs['objects']

    return results


def _mark_tree(objs, k, reffed):
    reffed.add(k)
    for attr, val in objs[k].items():
        if attr.endswith('_ref'):
            if val not in objs or val == k:
                continue
            _mark_tree(objs, val, reffed)
        elif attr.endswith('_refs'):
            for ref in val:
                if ref not in objs or ref == k:
                    continue
                _mark_tree(objs, ref, reffed)


def markroot(obj):
    """
    DEPRECATED
    If a SCO type appears multiple times in one observation, attempt
    to mark one as the "primary", or most significant, instance by
    setting an "x_root" attribute to 1

    The `makeid` transform was rolled into here as an optimization.

    """
    objs = obj['objects']
    reffed = set()

    # Keep track of the preference order of each reffed object, by type
    prefs = defaultdict(list)
    for idx, sco in objs.items():
        _set_id(sco)
        prefs[sco['type']].append(idx)
        for attr, val in sco.items():
            if attr.endswith('_ref'):
                if val not in objs or val == idx:
                    continue
                # If an object refs another object of the same type,
                # only mark the root (think process:parent_ref)
                if objs[idx]['type'] == objs[val]['type']:
                    _mark_tree(objs, val, reffed)
                elif (objs[val]['type'].endswith('-addr')):
                    if 'dst_' in attr:
                        # For src/dst pairs, consider the src as the root (so add dst to reffed)
                        reffed.add(val)
                    elif attr.endswith('src_ref'):
                        # Save ref as the "preferred" object for this type
                        prefs[objs[val]['type']].insert(0, val)
                elif val in reffed:
                    reffed.add(idx)
            elif attr.endswith('_refs'):
                for ref in val:
                    if ref not in objs or ref == idx:
                        continue
                    if objs[idx]['type'] == objs[ref]['type']:
                        reffed.add(ref)
    for k, v in objs.items():
        vtype = v['type']
        if k not in reffed:
            # Check if there's a more preferred object
            if vtype not in prefs:
                objs[k]['x_root'] = 1
            else:
                for i in prefs[vtype]:
                    if i in reffed:
                        continue
                    elif i == k:
                        objs[k]['x_root'] = 1
                    break
    return obj


def transform(ops, filename):
    for obj in get_objects(filename, ['identity', 'observed-data']):
        if obj['type'] == 'observed-data':
            inputs = [obj]
            for op in ops:  # making ops composable might be overkill (plus hard)
                results = []
                for i in inputs:
                    r = globals()[op](i)
                    if isinstance(r, list):
                        results.extend(r)
                    else:
                        results.append(r)
                inputs = results
        else:
            results = [obj]
        for result in results:
            yield result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('raft - streaming operations on STIX')
    parser.add_argument('op', metavar='OP')
    parser.add_argument('filename', metavar='FILENAME')
    args = parser.parse_args()
    for result in transform(args.op.split(','), args.filename):
        print(str(orjson.dumps(result), 'utf-8'))
