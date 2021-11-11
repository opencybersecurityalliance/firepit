import json
import os

import pytest
from stix2 import parse

from firepit.stix21 import makeid


class JsonStream:
    def __init__(self, filename):
        with open(filename, 'r') as fp:
            self.buf = fp.read()
        self.decoder = json.JSONDecoder()

    def __iter__(self):
        while self.buf:
            self.buf = self.buf.lstrip()
            try:
                obj, pos = self.decoder.raw_decode(self.buf)
            except json.decoder.JSONDecodeError:
                break
            self.buf = self.buf[pos:]
            yield obj


# Use examples from https://docs.oasis-open.org/cti/stix/v2.1/os/stix-v2.1-os.html
def pytest_generate_tests(metafunc):
    cwd = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(cwd, 'test_stix21_objects.json')
    if "sco21" in metafunc.fixturenames:
        metafunc.parametrize("sco21", JsonStream(filename))


def test_simple(sco21):
    # The example IDs are incorrect!
    # Use the official stix2 package to generate a valid ID
    sco = parse(json.dumps({k: v for k, v in sco21.items() if k != 'id'}))

    # Check that our generated ID matches what came out of the stix2 package
    assert makeid(sco21) == sco.id
