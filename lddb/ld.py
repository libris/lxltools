from __future__ import unicode_literals


GRAPH, ID, TYPE, REV = '@graph', '@id', '@type', '@reverse'


def flatten(data):
    result = []
    if isinstance(data, dict):
        data = [data]
    for part in data:
        _store_flattened(part, result)
    result.reverse()
    return {GRAPH: result}

def _store_flattened(current, result):
    if not isinstance(current, dict):
        return current
    flattened = _make_flat(current, result)
    if any(key for key in flattened if key != ID):
        result.append(flattened)
    itemid = current.get(ID)
    return {ID: itemid} if itemid else current

def _make_flat(obj, result):
    updated = {}
    for key, value in obj.items():
        if isinstance(value, list):
            value = [_store_flattened(o, result) for o in value]
        else:
            value = _store_flattened(value, result)
        updated[key] = value
    return updated


if __name__ == '__main__':
    import json
    import sys
    args = sys.argv[1:]
    fp = open(args.pop(0)) if args else sys.stdin
    data = json.load(fp)
    result = flatten(data)
    sys.stdout.write(json.dumps(result, indent=2, separators=(',', ': '),
            ensure_ascii=False).encode('utf-8'))
