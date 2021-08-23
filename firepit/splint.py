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


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


app = typer.Typer(
    name="splint",
    help="STIX processing and linting",
    add_completion=False,
)


TIME_FMT = '%Y-%m-%dT%H:%M:%S.%f'


def _timefmt(ts):
    return ts.strftime(TIME_FMT)[:-3] + 'Z'


def _start_bundle():
    bundle_id = 'bundle--' + str(uuid.uuid4())
    sys.stdout.write('{"type":"bundle",'
                     '"id": "')
    sys.stdout.write(bundle_id + '",')
    sys.stdout.write('"spec_version":"2.0",'
                     '"objects":[')


def _end_bundle():
    sys.stdout.write(']}')


@app.command()
def randomize_ids(
    filename: str = typer.Argument(..., help="STIX bundle file"),
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
                obj['modified'] = _timefmt(datetime.datetime.utcnow())
        blob = json.dumps(obj, separators=(',', ':'))
        if count:
            sys.stdout.write(f',{blob}')
        else:
            sys.stdout.write(f'{blob}')
        count += 1
    _end_bundle()


@app.command()
def dedup_ids(
    filename: str = typer.Argument(..., help="STIX bundle file"),
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
                obj['modified'] = _timefmt(datetime.datetime.utcnow())
        blob = json.dumps(obj, separators=(',', ':'))
        if count:
            sys.stdout.write(f',{blob}')
        else:
            sys.stdout.write(f'{blob}')
        count += 1
    _end_bundle()


@app.command()
def limit(
    n: int = typer.Argument(..., help="Max number of observations"),
    filename: str = typer.Argument(..., help="STIX bundle file"),
):
    """Truncate STIX bundle"""
    _start_bundle()
    count = 0
    for obj in raft.get_objects(filename):
        if count > n:
            break
        blob = json.dumps(obj, separators=(',', ':'))
        if count:
            sys.stdout.write(f',{blob}')
        else:
            sys.stdout.write(f'{blob}')
        count += 1
    _end_bundle()


if __name__ == "__main__":
    app()
