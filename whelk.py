#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from lddb.storage import Storage
from flask import Flask, request, abort, redirect, send_file


MIMETYPE_JSON = 'application/json'
MIMETYPE_JSONLD = 'application/ld+json'


app = Flask(__name__)
app.config.from_pyfile('config.cfg')


context_file = app.config.get('JSONLD_CONTEXT_FILE')
context_link = '</context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"'


storage = Storage('lddb', app.config['DBNAME'], app.config['DBHOST'],
        app.config['DBUSER'], app.config['DBPASSWORD'])


def _json_response(data):
    return json.dumps(data), 200, {'Content-Type': MIMETYPE_JSON, 'Link': context_link}

def _get_limit_offset(args):
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit and limit.isdigit():
        limit = int(limit)
    if offset and offset.isdigit():
        offset = int(offset)
    return limit, offset


@app.route('/relation')
def find_by_relation():
    rel = request.args.get('rel')
    ref = request.args.get('ref')
    limit, offset = _get_limit_offset(request.args)
    items = [x[1] for x in storage.find_by_relation(rel, ref, limit, offset)]
    return _json_response(items)

@app.route('/quotation')
def find_by_quotation():
    ref = request.args.get('ref')
    limit, offset = _get_limit_offset(request.args)
    items = [x[1] for x in storage.find_by_quotation(ref, limit, offset)]
    return _json_response(items)

@app.route('/context.jsonld')
def jsonld_context():
    return send_file(context_file, mimetype=MIMETYPE_JSONLD)

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
