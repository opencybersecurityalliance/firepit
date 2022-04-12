#!/usr/bin/env python

"""
Raft: Streaming processing of STIX Observation SDOs (`observed-data`).
"""

from collections import OrderedDict
from collections import defaultdict

import ijson
import requests
import ujson

from firepit import stix21


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
            bundle = ujson.loads(fp.read())
        yield from _yield_objects(bundle, types)


def _set_id(obs):
    sid = stix21.makeid(obs)
    obs['id'] = sid
    return sid


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
        if 'binary_ref' in sco:
            sco['image_ref'] = sco.pop('binary_ref')
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
            result['x_firepit_rank'] = rank



def flatten_21(obj):
    """
    For STIX 2.1 objects, "flatten" references
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


def flatten(obs):
    """
    Convert ref lists to objects, add ids if missing, etc.
    """
    if obs.get('spec_version', '2.0') == '2.1':
        return flatten_21(obs)

    if 'objects' not in obs:
        return [obs]

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
                    elif scos[val]['type'].endswith('-addr'):
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
