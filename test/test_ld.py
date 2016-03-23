from __future__ import unicode_literals
from os import path as P
import json
from lxltools import ld


def test_flatten():
    source = _load_json('flatten-001-in.jsonld')
    expected = _load_json('flatten-001-out.jsonld')
    result = ld.flatten(source)
    _check_json(result, expected)


def test_autoframe_flattened():
    _test_autoframe('flatten-001-out.jsonld', 'flatten-001-in.jsonld',
            "/record/something")

def test_autoframe_nested():
    _test_autoframe('frame-001-in.jsonld', 'frame-001-out.jsonld',
            "/record/something")

def _test_autoframe(sourcepath, expectedpath, rootid):
    source = _load_json(sourcepath)
    expected = _load_json(expectedpath)
    result = ld.autoframe(source, rootid)
    _check_json(result, expected)


def _load_json(ref):
    module_dir = P.dirname(__file__)
    path = P.join(module_dir, 'data', ref)
    with open(path) as fp:
        return json.load(fp)

def _check_json(result, expected):
    result_repr = json.dumps(result, indent=2, sort_keys=True)
    expected_repr = json.dumps(expected, indent=2, sort_keys=True)
    assert result_repr == expected_repr, "Got unexpected result: %s" % result_repr
