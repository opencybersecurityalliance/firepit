# type: ignore[attr-defined]

"""
Typer-based CLI for STIX processing and linting
"""

import datetime
import logging
import json
import os
import sys
import uuid

import typer

from firepit import raft
from firepit import woodchipper
from firepit.timestamp import KNOWN_TIMESTAMPS
from firepit.timestamp import timefmt
from firepit.timestamp import to_datetime


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


app = typer.Typer(
    name="splint",
    help="STIX processing and linting",
    add_completion=False,
)


def _start_bundle():
    bundle_id = 'bundle--' + str(uuid.uuid4())
    sys.stdout.write('{"type":"bundle","id":"%s","objects":[' % bundle_id)


def _dump_obj(obj, count):
    blob = json.dumps(obj, separators=(',', ':'), ensure_ascii=False)
    if count:
        sys.stdout.write(f',{blob}')
    else:
        sys.stdout.write(f'{blob}')


def _end_bundle():
    sys.stdout.write(']}')


@app.command()
def randomize_ids(
    filename: str = typer.Argument(..., help="STIX 2.0 bundle file"),
):
    """Randomize STIX observation IDs in a bundle"""
    _start_bundle()
    count = 0
    ds_id = None
    for obj in raft.get_objects(filename):
        if 'type' in obj:
            obj_type = obj['type']
            new_id = f'{obj_type}--{uuid.uuid4()}'
            if obj_type == 'identity' and not ds_id:
                ds_id = new_id
            else:
                assert ds_id, 'No identity object in bundle?'
                obj['created_by_ref'] = ds_id
            obj['id'] = new_id
            if 'modified' in obj:
                obj['modified'] = timefmt(datetime.datetime.utcnow())
        _dump_obj(obj, count)
        count += 1
    _end_bundle()


@app.command()
def dedup_ids(
    filename: str = typer.Argument(..., help="STIX 2.0 bundle file"),
):
    """Replace duplicate IDs with random IDs"""
    _start_bundle()
    count = 0
    ds_id = None
    ds_id_changed = False
    ids = set()
    for obj in raft.get_objects(filename):
        if 'type' in obj:
            old_id = obj.get('id', '')
            obj_type = obj['type']
            modified = False
            if old_id in ids:
                new_id = f'{obj_type}--{uuid.uuid4()}'
                obj['id'] = new_id
                modified = True
                ids.add(new_id)
            else:
                ids.add(old_id)
            if obj_type == 'identity' and not ds_id:
                ds_id = obj['id']
                if modified:
                    ds_id_changed = True
            elif ds_id_changed:
                assert ds_id, 'No identity object in bundle?'
                obj['created_by_ref'] = ds_id
                modified = True
            if 'modified' in obj and modified:
                obj['modified'] = timefmt(datetime.datetime.utcnow())
        _dump_obj(obj, count)
        count += 1
    _end_bundle()


@app.command()
def limit(
    n: int = typer.Argument(..., help="Max number of observations"),
    filename: str = typer.Argument(..., help="STIX 2.0 bundle file"),
):
    """Truncate STIX bundle"""
    _start_bundle()
    count = 0
    for obj in raft.get_objects(filename):
        if count > n:
            break
        _dump_obj(obj, count)
        count += 1
    _end_bundle()


@app.command()
def upgrade(
    filename: str = typer.Argument(..., help="STIX bundle file"),
):
    """Upgrade a STIX 2.0 bundle to 2.1"""
    _start_bundle()
    count = 0
    for obs in raft.get_objects(filename):
        for obj in raft.upgrade_2021(obs):
            _dump_obj(obj, count)
            count += 1
    _end_bundle()


def _shift_dt(dt, orig_start, new_start, scale):
    pos = (dt - orig_start)  # relative time in orig timeframe
    shift = datetime.timedelta(seconds=pos.total_seconds() * scale)
    return new_start + shift


def _shift_ts(ts, orig_start, new_start, scale, prec=3):
    dt = to_datetime(ts)
    new_dt = _shift_dt(dt, orig_start, new_start, scale)
    return timefmt(new_dt, prec)


@app.command()
def timeshift(
    filename: str = typer.Argument(..., help="STIX 2.0 bundle file"),
    start: str = typer.Argument(..., help="new start time"),
    end: str = typer.Argument(..., help="new end time"),
):
    """Timeshift STIX observations in a bundle"""
    _start_bundle()
    # Idea: 2 passes - first to get original timeframe, second to timeshift
    orig_start = None  # timefmt(datetime.datetime.utcnow())
    orig_end = None
    count = 0

    # First pass: get original timeframe
    for obj in raft.get_objects(filename):
        if 'type' in obj:
            obj_type = obj['type']
            if obj_type == 'observed-data':
                if count == 0:
                    orig_start = obj['first_observed']
                    orig_end = obj['first_observed']
                else:
                    orig_start = min(orig_start, obj['first_observed'])
                    orig_end = max(orig_end, obj['first_observed'])
                count += 1

    # Compute original duration
    ots0 = to_datetime(orig_start)
    ots1 = to_datetime(orig_end)
    orig_duration = ots1 - ots0

    # Compute new duration
    nts0 = to_datetime(start)
    nts1 = to_datetime(end)
    new_duration = nts1 - nts0

    scale = new_duration / orig_duration

    # Second pass: re-map timestamps
    count = 0
    for obj in raft.get_objects(filename):
        if 'type' in obj:
            obj_type = obj['type']
            if obj_type == 'observed-data':
                fo = to_datetime(obj['first_observed'])
                new_fo = _shift_dt(fo, ots0, nts0, scale)
                obj['first_observed'] = timefmt(new_fo)
                lo = to_datetime(obj['last_observed'])
                dur = lo - fo
                obj['last_observed'] = timefmt(new_fo + dur * scale)
                for sco in obj['objects'].values():
                    for prop, val in sco.items():
                        if prop in KNOWN_TIMESTAMPS:
                            sco[prop] = _shift_ts(val, ots0, nts0, scale, prec=6)
                if 'modified' in obj:
                    obj['modified'] = timefmt(datetime.datetime.utcnow())
        _dump_obj(obj, count)
        count += 1
    _end_bundle()


@app.command()
def convert(
    filename: str = typer.Argument(..., help="File to convert to STIX"),
):
    """Convert various log files to STIX 2.0 bundles"""
    woodchipper.convert(filename)


if __name__ == "__main__":
    app()
