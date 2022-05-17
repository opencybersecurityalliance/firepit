import json

from typer.testing import CliRunner

from firepit.cli import app


runner = CliRunner()


def test_cli_cache(fake_bundle_file):
    result = runner.invoke(app, ["cache", "test-id", fake_bundle_file])
    assert result.exit_code == 0


def test_cli_extract():
    result = runner.invoke(app, ["extract", "ips", "ipv4-addr", "test-id", "[ipv4-addr:value LIKE '%']"])
    assert result.exit_code == 0


def test_cli_filter():
    result = runner.invoke(app, ["filter", "filt_ips", "ipv4-addr", "ips", "[ipv4-addr:value LIKE '192.%']"])
    assert result.exit_code == 0    


def test_cli_lookup():
    result = runner.invoke(app, ["lookup", "ips", "--format=json", "--columns=value"])
    assert result.exit_code == 0
    output = json.loads(result.stdout)
    assert len(output) == 70
    assert set(output[0].keys()) == {"value"}
