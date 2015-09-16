#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from lddb.storage import Storage
from flask import Flask, request, abort, redirect, send_file


app = Flask(__name__)
app.config.from_pyfile('config.cfg')


storage = Storage('lddb', app.config['DBNAME'], app.config['DBHOST'],
        app.config['DBUSER'], app.config['DBPASSWORD'])


def _json_response(data):
    return json.dumps(data), 200, {'Content-Type': 'application/json'}

def _get_limit_offset(args):
    limit = args.get('limit')
    offset = args.get('offset')
    if limit and limit.isdigit():
        limit = int(limit)
    if offset and offset.isdigit():
        offset = int(offset)
    return limit, offset


@app.route('/find')
def find():
    #s = request.args.get('s')
    p = request.args.get('p')
    o = request.args.get('o')
    value = request.args.get('value')
    #language = request.args.get('language')
    #datatype = request.args.get('datatype')
    q = request.args.get('q')
    limit, offset = _get_limit_offset(request.args)
    records = []
    if p:
        if o:
            records = storage.find_by_relation(p, o, limit, offset)
        elif value:
            records = storage.find_by_value(p, value, limit, offset)
        elif q:
            records = storage.find_by_query(p, q, limit, offset)
    elif o:
        records = storage.find_by_quotation(o, limit, offset)
    items = [rec.data for rec in records]
    return _json_response(items)

@app.route('/favicon.ico')
def favicon():
    abort(404)

@app.route('/<path:path>')
def get_record(path):
    item_id = '/' + path
    record = storage.get_record(item_id)
    if record:
        data = {'@id': record.identifier}
        data.update(record.data)
        data.update(record.manifest)
        return _json_response(data)
    else:
        record_ids = list(storage.find_record_ids(item_id))
        if record_ids: #and len(record_ids) == 1:
            return redirect(record_ids[0], 303)
    abort(404)

def add(identifier, data, entry = {}):
    raise NotImplementedError
    # Pseudocode
    flat_data = ld.flatten(data)
    expanded_data = expander.expand(data)
    tripled_data = graphs.triplify(flat_data)
    try:
        storage.store(identifier, flat_data, entry)
        elastic.index(identifier, expanded_data)
        triplestore.update(identifer, tripled_data)
    except:
        storage.rollback()
        elastic.rollback()
    return identifier


if __name__ == '__main__':
    app.debug = app.config['DEBUG']
    app.run(host=app.config['BIND_HOST'], port=app.config['BIND_PORT'])
