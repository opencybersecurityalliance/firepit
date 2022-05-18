import json
import os

from typer.testing import CliRunner

from firepit.splint import app


runner = CliRunner()


def test_splint_limit(fake_bundle_file):
    result = runner.invoke(app, ["limit", "1", fake_bundle_file])
    assert result.exit_code == 0
    bundle = json.loads(result.stdout)
    objects = [o for o in bundle['objects']
               if o.get('type') == 'observed-data']
    assert len(objects) == 1


def test_splint_convert_csv():
    cwd = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(cwd, 'test_procs.csv')
    result = runner.invoke(app, ["convert", csv_file])
    assert result.exit_code == 0
    bundle = json.loads(result.stdout)
    assert bundle.get('type') == 'bundle'
    objects = [o for o in bundle['objects']
               if o.get('type') == 'observed-data']
    assert len(objects) == 5    


def test_splint_convert_sds():
    cwd = os.path.dirname(os.path.abspath(__file__))
    in_file = os.path.join(cwd, 'sds_example.json')
    result = runner.invoke(app, ["convert", in_file])
    assert result.exit_code == 0
    bundle = json.loads(result.stdout)
    assert bundle.get('type') == 'bundle'
    objects = [o for o in bundle['objects']
               if o.get('type') == 'observed-data']
    assert len(objects) == 2
    for obs in bundle['objects']:
        if obs.get('type') == 'observed-data':
            assert obs['first_observed'] in ("2019-11-16T12:59:17.131Z", "2019-11-16T12:59:11.273Z")
            assert obs['last_observed'] in ("2019-11-16T12:59:17.131Z", "2019-11-16T12:59:11.273Z")
            assert obs['number_observed'] == 1
            for sco in obs['objects']:
                if obs.get('type') == 'file':
                    assert sco['name'] in ('conhost.exe', 'wdsync-inotify.exe')


def test_splint_convert_zeek():
    cwd = os.path.dirname(os.path.abspath(__file__))
    in_file = os.path.join(cwd, 'zeek_example.log')
    result = runner.invoke(app, ["convert", in_file])
    assert result.exit_code == 0
    bundle = json.loads(result.stdout)
    assert bundle.get('type') == 'bundle'
    objects = [o for o in bundle['objects']
               if o.get('type') == 'observed-data']
    assert len(objects) == 2
