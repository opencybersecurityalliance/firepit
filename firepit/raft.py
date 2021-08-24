#!/usr/bin/env python

"""
Raft: Streaming processing of STIX data.  EXPERIMENTAL - you
probably don't want to use this for anything serious.
"""

import re
from collections import OrderedDict
from collections import defaultdict

import ijson
import orjson
import ujson
import requests

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
        yield from _yield_objects(source, types)
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


def _is_ref(name):
    return name.endswith('_ref') \
        or name.endswith('_refs')


def _add_to_refs(obj, prop, val):
    if prop not in val:
        obj[prop] = [val]
    else:
        obj[prop].append(val)


def _set_id(idx, obs, oid):
    stype = obs['type']
    uid = oid.lstrip('observed-data')
    obs['id'] = f'{stype}--{uid}_{idx}'
    return uid


def preserve(obj):
    '''Stash the "raw" STIX JSON as a stirng attribute in the object itself'''
    if obj['type'] == 'observed-data':
        obj['x_stix'] = ujson.dumps(obj)
    return [obj]


def promote(obj):
    '''Given an Observation, "promote" contained SCOs to top level'''
    # TODO: also "promote"/synthesize SROs for references?
    if 'objects' not in obj:
        return [obj]
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
            uid = _set_id(idx, obs, oid)
            for prop, val in obs.items():
                if _is_ref(prop):
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
    '''Add unique object IDs to SCOs inside an Observation SDO'''
    if 'objects' not in obj:
        return [obj]
    oid = obj['id']
    observables = obj.get('objects', {})
    for idx, obs in observables.items():
        _set_id(idx, obs, oid)
    return [obj]


def _resolve(obs_orig, observables):
    obs = {}
    for prop, val in obs_orig.items():
        if _is_ref(prop) and prop != 'child_refs':
            id_only = False
            if _is_custom_obj(obs_orig) and not _is_custom_prop(prop):
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
    if 'objects' not in obj:
        return [obj]
    object_refs = []
    observables = obj.get('objects', {})
    for idx, obs_orig in observables.items():
        object_refs.append(_resolve(obs_orig, observables))
    del obj['objects']
    obj['objects'] = object_refs
    return [obj]


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
        key = prefix + sep + key if prefix else key
        if isinstance(v, dict):
            r.update(json_normalize(v, key, sep, flat_lists))
        elif flat_lists and isinstance(v, list):
            for i, val in enumerate(v):
                r[key + f'[{i}]'] = val
        else:
            r[key] = v
    return r


def normalize(obj):
    """Normalize obj to a flat dict"""
    return [json_normalize(obj, flat_lists=False)]


def invert(obj):
    '''Invert reference lists so all refs are 1:1'''
    if obj['type'] != 'observed-data':
        return [obj]

    objects = obj['objects']

    for k, v in objects.items():
        reflists = []
        for attr, val in v.items():
            if attr.endswith('_refs') and isinstance(val, list):
                refname = re.sub(r'([a-z0-9_-]*)_refs', r'x_\g<1>_of_ref', attr)
                refname = INVERTED_REFS.get(refname, refname)
                for ref in val:
                    if ref not in objects or ref == k:  # Detect bogus refs
                        continue
                    target = objects[ref]
                    target[refname] = k
                reflists.append(attr)

        for attr in reflists:
            # Record the count of the ref list we've inverted
            v[re.sub(r'([a-z0-9_-]*)_refs', r'x_\g<1>_count', attr)] = len(v[attr])
            del v[attr]

    return [obj]


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


def markroot(obj, viewname='observed-data'):
    if obj['type'] != 'observed-data':
        return [obj]
    objs = obj['objects']
    reffed = set()

    # Keep track of the preference order of each reffed object, by type
    prefs = defaultdict(list)
    for idx, sco in objs.items():
        prefs[sco['type']].append(idx)
        for attr, val in json_normalize(sco, flat_lists=False).items():
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
        if k not in reffed:
            # Check if there's a more preferred object
            if v['type'] not in prefs or prefs[v['type']][0] == k:
                objs[k]['x_root'] = 1
    return [obj]


def transform(ops, filename, op_arg=None):
    for obj in get_objects(filename, ['identity', 'observed-data']):
        inputs = [obj]
        for op in ops:  # making ops composable might be overkill (plus hard)
            results = []
            for i in inputs:
                if op_arg:
                    results.extend(globals()[op](i, op_arg))
                else:
                    results.extend(globals()[op](i))
            inputs = results
        for result in results:
            yield result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('raft - streaming operations on STIX')
    parser.add_argument('op', metavar='OP')
    parser.add_argument('op_arg', metavar='ARG', nargs='?')  # Goofy
    parser.add_argument('filename', metavar='FILENAME')
    args = parser.parse_args()
    for result in transform(args.op.split(','), args.filename, args.op_arg):
        print(str(orjson.dumps(result), 'utf-8'))
