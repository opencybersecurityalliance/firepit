import base64
import csv
import datetime
import json
import ntpath
import re
import socket
import sys
import uuid
import zipfile
from collections import OrderedDict
from ipaddress import ip_address

import dateutil.parser

from firepit.props import primary_prop
from firepit.props import ref_type
from firepit.timestamp import KNOWN_TIMESTAMPS
from firepit.timestamp import timefmt


## Code for generating STIX from intermediate format


INTEGER_PROPS = {
    # autonomous-system
    'number',

    # file (and others)
    'size',

    # network-traffic
    'src_port',
    'dst_port',
    'src_byte_count',
    'dst_byte_count',
    'src_packets',
    'dst_packets',

    # process
    'pid',
}


REG_HIVE_MAP = {
    'HKLM': 'HKEY_LOCAL_MACHINE',
    'HKCU': 'HKEY_CURRENT_USER',
    'HKCR': 'HKEY_CLASSES_ROOT',
    'HKCC': 'HKEY_CURRENT_CONFIG',
    'HKPD': 'HKEY_PERFORMANCE_DATA',
    'HKU':  'HKEY_USERS',
    'HKDD': 'HKEY_DYN_DATA',
}


def guess_ref_type(prop, val):
    """Get data type for `sco_type`:`prop` reference"""
    rtypes = ref_type(None, prop)
    rtype = rtypes[0] if len(rtypes) > 0 else None  # FIXME
    if rtype == 'ipv4-addr' and ':' in val:
        rtype = 'ipv6-addr'
    if rtype is None:
        # just guess based on value
        if re.match(r'([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}', val):
            rtype = 'mac-addr'
    return rtype


def recreate_dict(obj, prop, rest, val):
    thing = obj.get(prop, {})
    first, _, rest = rest.partition('.')
    if not rest:
        thing[first.strip("'")] = val
    else:
        recreate_dict(thing, first, rest, val)
    obj[prop.strip("'")] = thing


def format_val(sco_type, prop, val):
    if prop in KNOWN_TIMESTAMPS:
        ts = dateutil.parser.parse(val)
        result = timefmt(ts)
    elif prop in INTEGER_PROPS:
        try:
            result = int(val)
        except ValueError:
            result = int(val, 16)
    elif prop == 'protocols':  # HACKY
        result = [val]
    elif prop == 'key':
        for abbrev, full in REG_HIVE_MAP.items():
            if val.startswith(abbrev):
                result = val.replace(abbrev, full, 1)
        result = val
    elif sco_type == 'ipv4-addr' and prop == 'value':
        # DNS QueryResults have ; at the end of addr?
        result = val.strip(';')  # TODO: need to check for multiple addrs?
    else:
        result = val
    return result


def set_obs_prop(observable, path, val, scos, key):
    prop, _, rest = path.partition('.')
    if prop.endswith(']'):  #FIXME: not always a ref!
        ref_name, _, idx = prop.rstrip(']').partition('[')
        ref_type = guess_ref_type(ref_name, val)
        ref_key = key + prop
        other = scos.get(ref_key, {'type': ref_type})
        if '.' in rest:
            set_obs_prop(other, rest, val, scos, ref_key + '.')
        else:
            other[rest] = format_val(ref_type, rest, val)
        scos[ref_key] = other
        if ref_name in observable:
            refs = observable[ref_name]
            refs.append(ref_key)
        else:
            refs = [None for i in range(int(idx) + 1)]
            refs[int(idx)] = ref_key
            observable[ref_name] = refs
    elif prop.endswith('_ref') or prop.endswith('_refs'):
        ref_type = guess_ref_type(prop, val)
        ref_key = key + prop
        other = scos.get(ref_key, {'type': ref_type})
        if '.' in rest:
            set_obs_prop(other, rest, val, scos, ref_key + '.')
        else:
            other[rest] = format_val(ref_type, rest, val)
        scos[ref_key] = other
        observable[prop] = ref_key
    elif not rest:
        observable[prop] = format_val(observable['type'], prop, val)
    elif '_refs[*].' in rest:
        # TODO
        # Trying to deal with e.g. extensions.'dns-ext'.resolved_ip_refs[*].value
        pass
    elif '_ref.' in rest:
        # I don't think this is working yet
        # Trying to deal with e.g. extensions.'dns-ext'.question.name_ref.value
        thing = observable.get(prop, {})
        if key.endswith(':') or key.endswith('.'):
            ref_key = key + prop + '.'
        else:
            ref_key = key + prop
        set_obs_prop(thing, rest, val, scos, ref_key)
        observable[prop.strip("'")] = thing
    else:
        recreate_dict(observable, prop, rest, val)


def fixup_hashes(hashes: dict):
    result = []
    for key, val in hashes.items():
        key = key.replace('SHA', 'SHA-')
        if '-' in key:
            key = f"'{key}'"
        result.append(('process:binary_ref.hashes.' + key, val))
    return result


def _translate_refs(obj, mapping):
    combos = {}
    for prop, val in obj.items():
        if isinstance(val, dict):
            combos.update(_translate_refs(val, mapping))
        if prop.endswith('_ref') or prop.endswith('_refs'):
            if isinstance(val, list):
                obj[prop] = [mapping[v] for v in val]
            else:
                obj[prop] = mapping[val]
        elif prop.endswith(']'):
            stub, _, _ = prop.partition('[')
            if stub not in combos:
                combos[stub] = []
            combos[stub].append(mapping[val])
    return combos


def dict2observation(creator, row):
    now = timefmt(datetime.datetime.utcnow())
    od = OrderedDict(
        {
            'type': 'observed-data',
            'id': 'observed-data--' + str(uuid.uuid4()),
            'created_by_ref': creator['id'],
            'created': now,
            'modified': now,
            'number_observed': 1,
        }
    )

    scos = {}  # TODO: need a graph/tree instead?
    for key, val in row.items():
        if not val:
            continue
        if '#' in key:
            key, _, sco_name = key.partition('#')
        else:
            sco_name = None
        if ':' not in key:  # Not STIX object path -> property of observed-data
            if '.' not in key:
                if key in KNOWN_TIMESTAMPS:
                    ts = dateutil.parser.parse(val)
                    od[key] = timefmt(ts)
                else:
                    od[key] = val
            else:
                prop, _, rest = key.partition('.')
                recreate_dict(od, prop, rest, val)
        else:
            sco_type, _, rest = key.partition(':')
            sco_key = sco_name or sco_type
            observable = scos.get(sco_key, {'type': sco_type})
            set_obs_prop(observable, rest, val, scos, sco_type + ':')
            scos[sco_key] = observable
    od['objects'] = {}

    # Create a mapping from ref_key to index num
    mapping = {}
    for key, obj in scos.items():
        idx = len(od['objects'])
        od['objects'][str(idx)] = obj
        mapping[key] = str(idx)

    # Translate references
    repls = {}
    for key, obj in od['objects'].items():
        combos = _translate_refs(obj, mapping)

        # Combine references
        for k, v in combos.items():
            obj[k] = v
        new_obj = {k: v for k, v in obj.items() if not k.endswith(']')}
        repls[key] = new_obj
    for orig, repl in repls.items():
        od['objects'][orig] = repl

    # Walk objects and fix up x-oca-event if present
    refs = {}
    for idx, sco in od['objects'].items():
        sco_type = sco['type']
        if sco_type == 'network-traffic':
            refs[sco_type] = idx
        elif sco_type == 'process':  #'opened_connection_refs' in sco:
            if 'process' not in refs:
                refs[sco_type] = idx
            if 'parent_ref' in sco:
                refs[sco_type] = idx
                refs['parent_process'] = sco['parent_ref']
        elif sco_type == 'domain-name':
            refs[sco_type] = idx
        elif sco_type == 'file':
            refs[sco_type] = idx
        elif sco_type == 'x-oca-event':
            refs[sco_type] = idx
        elif sco_type == 'x-oca-asset':
            refs[sco_type] = idx

    if 'x-oca-event' in refs:
        event = od['objects'][refs['x-oca-event']]
        for sco_type, idx in refs.items():
            if sco_type == 'network-traffic':
                event['network_ref'] = idx
            elif sco_type in ['process', 'parent_process']:
                event[sco_type + '_ref'] = idx
            elif sco_type == 'file' and is_file_event(event['code']):
                event[sco_type + '_ref'] = idx
            elif sco_type == 'domain-name':
                event['domain_ref'] = idx
            elif sco_type == 'x-oca-asset':
                event['host_ref'] = idx

    return od


## End of STIX generation code


## Code for creating intermediate format

def from_unix_time(ts):
    if isinstance(ts, str):
        ts = float(ts)
    ts = datetime.datetime.fromtimestamp(ts).isoformat().replace('+00:00', 'Z')
    return [('first_observed', ts),
            ('last_observed', ts)]


def to_action_code(event_id):
    '''Convert windows event ID to x-oca-event action and code'''
    event_id = int(event_id)
    return [
        ('x-oca-event:code', event_id),
        ('x-oca-event:action', windows_events.get(event_id)),
    ]


def to_payload_bin(value):
    return [
        ('artifact:payload_bin', base64.b64encode(value.encode()).decode('ascii'))
    ]


PROTO_TABLE = {num:name[8:] for name, num in vars(socket).items() if name.startswith("IPPROTO")}
def to_protocol(value):
    if value.isdigit():
        try:
            value = PROTO_TABLE[int(value)].lower()
        except KeyError:
            pass
    return [
        ("process:opened_connection_refs[0].protocols", value)
    ]


def is_file_event(event_id):
    return event_id in {6, 7, 9, 11, 15}


def split_hash(hash_string: str):
    token_dict = {
        "SHA1=": "process:binary_ref.hashes.'SHA-1'",
        "MD5=": "process:binary_ref.hashes.MD5",
        "SHA256=": "process:binary_ref.hashes.'SHA-256'"
    }
    hashes = []
    for hstr in hash_string.split(','):
        for hash_token, _stix_key in token_dict.items():
            if hash_token in hstr:
                hashes += [(token_dict[hash_token], hstr[len(hash_token):])]
    return hashes


def split_image(abs_name: str, prefix='process:'):
    name = ntpath.basename(abs_name)
    path = ntpath.dirname(abs_name)
    return [
        (prefix + 'name', name),
        (prefix + 'binary_ref.name', name),
        (prefix + 'binary_ref.parent_directory_ref.path', path)
    ]


def split_parent_image(abs_name: str):
    return split_image(abs_name, prefix='process:parent_ref.')


def split_image_loaded(abs_name: str):
    name = ntpath.basename(abs_name)
    path = ntpath.dirname(abs_name)
    return [
        ('file:name#loaded', name),
        ('file:parent_directory_ref.path#loaded', path)
    ]


def split_reg_key_value(path: str):
    key, _, value = path.rpartition('\\')
    return [
        ('windows-registry-key:key', key),
        ('windows-registry-key:values', [{'name': value}]),
    ]


# Do we need this?  Or can we extract it from the Message field?
windows_events = {
    1: 'Process Creation',
    2: 'Process Changed a file creation time',
    3: 'Network Connection',
    4: 'Sysmon Service State Change',
    5: 'Process Terminated',
    6: 'Driver Loaded',
    7: 'Image Loaded',
    8: 'Create Remote Thread',
    9: 'Raw File Access Read',
    10: 'Process Access',
    11: 'File Create',
    12: 'Registry Create and Delete',
    13: 'Registry Value Set',
    14: 'Registry Key and Value Rename',
    15: 'File Create Stream Hash',
    16: 'Sysmon Config Change',
    17: 'Pipe Event Created',
    18: 'Pipe Event Connected',
    19: 'WMI EventFilter activity',
    20: 'WMI EventConsumer activity',
    21: 'WMI EventConsumerToFilter activity',
    22: 'DNS Query',
    255: 'Sysmon error',
}


# Specialized mappings per Windows EventID
# We should probably moved the shared ones out of here.
# Only need this for properties whose meaning depends on ID.
windows_mapping = {
    1: {
        "UtcTime": ["first_observed", "last_observed", "process:created"],
        "Image": split_image,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
        "CommandLine": ["process:command_line"],
        "ParentImage": split_parent_image,
        "ParentProcessId": "process:parent_ref.pid",
        "ParentProcessGuid": "process:parent_ref.x_unique_id",
        "ParentCommandLine": "process:parent_ref.command_line",
        #"UserID": "process:creator_user_ref.user_id",
        #"User": "process:creator_user_ref.account_login",
        "User": "process:creator_user_ref.user_id",
        "Hashes": split_hash,
    },
    3: {
        "UtcTime": ["first_observed", "last_observed"],
        "Image": split_image,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
        "SourceIp": "process:opened_connection_refs[0].src_ref.value",
        "DestinationIp": "process:opened_connection_refs[0].dst_ref.value",
        "Protocol": "process:opened_connection_refs[0].protocols",
        "SourcePort": "process:opened_connection_refs[0].src_port",
        "DestinationPort": "process:opened_connection_refs[0].dst_port",
    },
    5: {
        "UtcTime": ["first_observed", "last_observed"],
        "Image": split_image,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
    },
    7: {
        "UtcTime": ["first_observed", "last_observed", "process:created"],
        "Image": split_image,
        "ImageLoaded": split_image_loaded,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
        "CommandLine": ["process:command_line"],
        "Hashes": split_hash,
    },
    12: {
        "UtcTime": ["first_observed", "last_observed"],
        "Image": split_image,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
        "TargetObject": "windows-registry-key:key",  # OR: "process:x_created_key_ref.key"?
    },
    13: {
        "UtcTime": ["first_observed", "last_observed"],
        "Image": split_image,
        "ProcessId": "process:pid",
        "ProcessGuid": "process:x_unique_id",
        "TargetObject": split_reg_key_value,
    },
    3018: {
        "QueryName": "domain-name:value",
        #"QueryType": "domain-name:resolves_to_refs[0].type",
        "QueryResults": "domain-name:resolves_to_refs[0].value",
    },
    4688: {
        "NewProcessName": split_image,
        "NewProcessId": "process:pid",
        "CommandLine": ["process:command_line"],
        "ParentProcessName": split_parent_image,
        "ParentProcessGuid": "process:parent_ref.x_unique_id",
        "ProcessId": "process:parent_ref.pid",
        "ProcessGuid": "process:x_unique_id",
        #"SubjectUserName": "process:creator_user_ref.account_login",
        "SubjectUserName": "process:creator_user_ref.user_id",
    },
    5156: {
        "Application": split_image,
        "TimeCreated": ["first_observed", "last_observed"],
        "ProcessId": "process:pid",
        "SourceAddress": "process:opened_connection_refs[0].src_ref.value",
        "SourcePort": "process:opened_connection_refs[0].src_port",
        "DestAddress": "process:opened_connection_refs[0].dst_ref.value",
        "DestPort": "process:opened_connection_refs[0].dst_port",
        "Protocol": to_protocol,
    },
}


def merge_mappings(common, specific, key=None):
    '''Merge common mapping into specific[key] mapping'''
    if key:
        return {k: {j: {**u, **common} if j == key else u for j, u in v.items()} for k, v in specific.items()}
    return {**common, **specific}


def process_mapping(event, mapping):
    tuples = []
    for map_key, map_val in mapping.items():
        if isinstance(map_val, dict):
            for inner_key, inner_val in map_val.items():
                tuples += process_mapping(event[inner_key], inner_val)
        elif isinstance(map_val, list):
            event_val = event.get(map_key)
            if event_val is not None:
                for inner_key in map_val:
                    tuples += [(inner_key, event_val)]
        elif isinstance(map_val, str):
            event_val = event.get(map_key)
            if event_val is not None:
                tuples += [(map_val, event_val)]
        elif callable(map_val):
            event_val = event.get(map_key)
            if event_val is not None:
                tuples += map_val(event_val)
    return tuples


def process_event(event, mapping, event_id=None):
    if event_id:
        # If we have a Windows event, merge the mappings
        event_mapping = windows_mapping.get(event_id)
        if event_mapping:
            mapping = merge_mappings(mapping, event_mapping)
    return dict(process_mapping(event, mapping))


## End of code for creating intermediate format

class Mapper:
    def detect(self, event):
        raise NotImplementedError

    def convert(self, event):
        raise NotImplementedError

## Datasource specific code


# Security Datasets - https://github.com/OTRF/Security-Datasets
class SdsMapper(Mapper):

    @staticmethod
    def enhanced_action(message):
        results = to_payload_bin(message)
        m = re.search(r'^([^:\.]*)', message)
        if m:
            results.append(('x-oca-event:action', m.group(1)))
        m = re.search(r'EventType: (\w+)', message)
        if m:
            event_type = m.group(1)
            event_id = SdsMapper.event_types.get(event_type)
            if event_id:
                results.append(('x-oca-event:action', windows_events.get(event_id) + ' - ' + event_type))
        m = re.search(r'Details: ([^"]*)', message)
        if m:
            details = m.group(1)
            if details.startswith('DWORD') or details.startswith('QWORD'):
                parts = details.split()
                results.append(('windows-registry-key:values', [{'data': parts[1], 'data_type': parts[0]}]))
        return results

    # TODO: Are these common to all Windows event sources?
    common_mapping = {
        "@timestamp": ["first_observed", "last_observed"],
        "TimeCreated": ["first_observed", "last_observed"],
        "Channel": "x-oca-event:module",
        "SourceName": "x-oca-event:provider",
        "Hostname": "x-oca-asset:hostname",
        "EventID": to_action_code,
        "Category": "x-oca-event:category",
        "Message": lambda x: SdsMapper.enhanced_action(x),
        #"Message": to_payload_bin,
        "ProcessName": split_image,  # At least some events use this instead of Image
        "ProcessId": "process:pid",
        "Application": split_image,  # At least some events use this instead of Image
    }

    # Mapping of EventType message field to event ID
    event_types = {
        'SetValue': 13,
        'DeleteValue': 12,
        'CreateKey': 12,
        'DeleteKey': 12,
        'CreatePipe': 17,
        'ConnectPipe': 18,
    }

    def detect(self, event):
        tags = event.get('tags')
        return ((tags is not None and 'mordorDataset' in tags) or
                ('EventID' in event and 'TimeCreated' in event))  # FIXME: too generic?

    def convert(self, event):
        event_id = event['EventID']
        result = process_event(event, self.common_mapping, event_id)
        #if 'user-account:account_login' not in result:
        if 'user-account:user_id' not in result:
            username = event.get('TargetUserName')
            if not username:
                username = event.get('SubjectUserName')
            if username and username != '-':
                #result['user-account:account_login'] = username
                result['user-account:user_id'] = username
        return result


# Zeek logs
# The problem here is that zeek logs span multiple files; this only covers conn OR dns.
# TODO: figure out how to merge the different Zeek logs first, then process.

class ZeekCsvMapper(Mapper):
    zeek_mapping = {  # FIXME: this is only conn log
        "ts": from_unix_time,
        "id.orig_h": "network-traffic:src_ref.value",
        "id.orig_p": "network-traffic:src_port",
        "orig_ip_bytes": "network-traffic:src_byte_count",
        "orig_pkts": "network-traffic:src_packets",
        "id.resp_h": "network-traffic:dst_ref.value",
        "id.resp_p": "network-traffic:dst_port",
        "resp_ip_bytes": "network-traffic:dst_byte_count",
        "resp_pkts": "network-traffic:dst_packets",
        "proto": "network-traffic:protocols",
    }

    def detect(self, event):
        return 'id.orig_h' in event

    def convert(self, event):
        return dict(process_event(event, self.zeek_mapping))


class ZeekJsonMapper(Mapper):
    common_mapping = {
        #"@system": "x-oca-asset:hostname",
        "ts": from_unix_time,
        "id_orig_h": "network-traffic:src_ref.value",
        "id_orig_p": "network-traffic:src_port",
        "id_resp_h": "network-traffic:dst_ref.value",
        "id_resp_p": "network-traffic:dst_port",
        "proto": "network-traffic:protocols",
    }

    zeek_mapping = {
        'conn': {
            "orig_ip_bytes": "network-traffic:src_byte_count",
            "resp_ip_bytes": "network-traffic:dst_byte_count",
            "orig_pkts": "network-traffic:src_packets",
            "resp_pkts": "network-traffic:dst_packets",
            "orig_l2_addr": "network-traffic:src_ref.resolves_to_refs[0].value",
            "resp_l2_addr": "network-traffic:dst_ref.resolves_to_refs[0].value",
        },
        'dns': {
            #'query': "network-traffic:extensions.'dns-ext'.question.name_ref.value",
            'query': 'domain-name:value',
            'answers': lambda x: ZeekJsonMapper.process_answers(x),
        }
    }

    @staticmethod
    def process_answers(answers):
        results = []
        i = 0
        for answer in answers:
            try:
                _ = ip_address(answer)
                #results.append(("network-traffic:extensions.'dns-ext'.resolved_ip_refs[*].value", answer))
                results.append((f"domain-name:resolves_to_refs[{i}].value", answer))
                i += 1
            except ValueError:
                pass
        return results

    def detect(self, event):
        return '@stream' in event

    def convert(self, event):
        stream = event['@stream']
        if stream in self.zeek_mapping:
            mapping = merge_mappings(self.common_mapping, self.zeek_mapping[stream])
        else:
            mapping = self.common_mapping
        return dict(process_event(event, mapping))


# ISC Honeypot: e.g. https://isc.sans.edu/api/#webhoneypotreportsbyurl

class IscHoneypotJsonMapper(Mapper):
    mapping = {
        "url": "url:value",
        "user_agent": "network-traffic:extensions.'http-request-ext'.request_header.'User-Agent'",
        "source": "network-traffic:src_ref.value",
        "ts": ["first_observed", "last_observed"],
        "sport": "network-traffic:src_port",
        "dport": "network-traffic:dst_port",
        "dest": "network-traffic:dst_ref.value",
        "proto": "network-traffic:protocols",
    }

    def detect(self, event):
        return 'url' in event and 'user_agent' in event and 'source' in event

    def convert(self, event):
        # ISC Honeypot doesn't have ports or dest addr, so make them up
        event['sport'] = 0
        event['dport'] = 80
        event['dest'] = '127.0.0.1'
        event['proto'] = 'tcp'
        event['ts'] = event['date'] + 'T' + event['time'] + '.000Z'
        return dict(process_event(event, self.mapping))


# Generic "flat" JSON mapper

class FlatJsonMapper(Mapper):
    def detect(self, event):
        otype = event.get('type')
        if otype:
            return primary_prop(otype) in event
        return False

    def convert(self, event):
        result = {}
        otype = event.get('type')
        timestamp_key = None
        if otype:
            for key, value in event.items():
                if key in ['first_observed', 'last_observed', 'number_observed']:
                    new_key = key
                else:
                    new_key = f'{otype}:{key}'
                    if key in KNOWN_TIMESTAMPS:
                        timestamp_key = key
                result[new_key] = value
            if timestamp_key and 'first_observed' not in result:
                ts = event[timestamp_key]
                result['first_observed'] = ts
                result['last_observed'] = ts
            return result
        return None


# TODO: "register" each data type


# File format code

def process_events(events, mappers, ident):
    mapper = None
    results = []
    for event in events:
        if not isinstance(event, dict):
            continue
        if not mapper:
            # Detect data type
            for m in mappers:
                if m.detect(event):
                    mapper = m
                    break
        if mapper:
            od = mapper.convert(event)
            if od:
                results.append(dict2observation(ident, od))
    return results


def read_csv(fp, mappers, ident):
    # Currently this knows about Bro/Zeek CSV format
    # Ideally this would be agnostic to the CSV producer
    quoting = csv.QUOTE_NONE
    sep = '\t'
    linenum = 0
    for line in fp:
        line = line.rstrip('\n')
        if line.startswith('#separator'):
            _, _, sep = line.partition(' ').decode('unicode_escape')
        elif line.startswith('#fields'):
            names = line[1:].split(sep)[1:]
        elif line.startswith('#types'):
            break
        elif not line.startswith('#') and linenum == 0:
            if sep not in line:
                # If not tab, assume comma.
                sep = ','
            # Determine fieldnames from header
            # Also try to infer quoting style
            names = []
            quoting = csv.QUOTE_NONNUMERIC
            for name in line.split(sep):
                if name.startswith('"'):
                    name = name.strip('"')
                    if name.isdigit():
                        quoting = csv.QUOTE_ALL
                        break
                names.append(name)
            break
    reader = csv.DictReader(fp, delimiter=sep, fieldnames=names, quoting=quoting)
    events = []
    for obj in reader:
        if obj.get('ts') == '#close':  # Weird Zeek thing
            break
        events.append(obj)
    return process_events(events, mappers, ident)


def read_json(fp, mappers, ident):
    try:
        data = json.load(fp)
    except:
        fp.seek(0)
        data = (json.loads(line) for line in fp)
    return process_events(data, mappers, ident)


def read_log(fp, mappers, ident):
    try:
        data = (json.loads(line) for line in fp)
        result = process_events(data, mappers, ident)
    except json.decoder.JSONDecodeError:
        result = read_csv(fp, mappers, ident)
    return result


def detect_filetype(input_file):
    if input_file.endswith('.csv'):
        read_func = read_csv
    elif input_file.endswith('.json'):
        read_func = read_json
    elif input_file.endswith('.log'):
        read_func = read_log
    else:
        raise NotImplementedError
    return read_func


def convert(input_file):
    now = timefmt(datetime.datetime.utcnow())
    id1 = OrderedDict({
        "type": "identity",
        "identity_class": "program",
        "name": "woodchipper",  # TODO: pass this in as arg
        "id": "identity--" + str(uuid.uuid4()),
        "created": now,
        "modified": now,
    })

    mappers = [
        SdsMapper(),
        ZeekJsonMapper(),
        ZeekCsvMapper(),
        IscHoneypotJsonMapper(),
        FlatJsonMapper(),
    ]

    # TODO: STIX 2.1
    bundle = {
        'type': 'bundle',
        'id': 'bundle--' + str(uuid.uuid4()),
        'objects': []
    }
    objects = [id1]

    try:
        if input_file.endswith('.zip'):
            zf = zipfile.ZipFile(input_file)
            for filename in zf.namelist():
                try:
                    read_func = detect_filetype(filename)
                    input_file = filename
                    break
                except NotImplementedError:
                    pass
            fp = zf.open(input_file, 'r')
        else:
            read_func = detect_filetype(input_file)
            fp = open(input_file, 'r')

        objects += read_func(fp, mappers, id1)
    except Exception as e:
        fp.close()
        raise e

    bundle['objects'] = objects
    print(json.dumps(bundle, indent=4, ensure_ascii=False))


if __name__ == '__main__':
    convert(sys.argv[1])
