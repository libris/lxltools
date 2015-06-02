from __future__ import unicode_literals


GRAPH, ID, TYPE, REV = '@graph', '@id', '@type', '@reverse'


def flatten(data, objects=None):
    result = [] if objects is None else objects
    if isinstance(data, dict):
        data = [data]
    for part in data:
        store_flattened(part, result)
    result.reverse()
    return {GRAPH: result}

def store_flattened(current, result):
    if not isinstance(current, dict):
        return current
    flattened = make_flat(current, result)
    result.append(flattened)
    itemid = current.get(ID)
    return {ID: itemid} if itemid else current

def make_flat(obj, result):
    updated = {}
    for key, value in obj.items():
        if isinstance(value, list):
            value = [store_flattened(o, result) for o in value]
        else:
            value = store_flattened(value, result)
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
