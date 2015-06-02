from os import path as P
import json
from lddb import ld

def test_flatten():
    source = get_data('flatten-001-in.jsonld')
    expected = get_data('flatten-001-out.jsonld')
    result = ld.flatten(source)
    assert result == expected


def get_data(ref):
    module_dir = P.dirname(__file__)
    path = P.join(module_dir, 'data', ref)
    with open(path) as fp:
        return json.load(fp)
