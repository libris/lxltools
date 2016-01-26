from __future__ import unicode_literals
from os import path as P
import json
from lxltools import ld


def test_flatten():
    source = _load_json('flatten-001-in.jsonld')
    expected = _load_json('flatten-001-out.jsonld')
    result = ld.flatten(source)
    assert result == expected


def test_autoframe():
    source = _load_json('flatten-001-out.jsonld')
    expected = _load_json('flatten-001-in.jsonld')
    result = ld.autoframe(source, "/record/something")
    assert result == expected


def _load_json(ref):
    module_dir = P.dirname(__file__)
    path = P.join(module_dir, 'data', ref)
    with open(path) as fp:
        return json.load(fp)
