#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from lddb import Storage
from flask import Flask, request, abort, redirect, send_file


MIMETYPE_JSON = 'application/json'
MIMETYPE_JSONLD = 'application/ld+json'


app = Flask(__name__)
app.config.from_pyfile('config.cfg')
app.secret_key = app.config.get('SESSION_SECRET_KEY')

context_file = app.config.get('JSONLD_CONTEXT_FILE')
context_link = '</context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"'

storage = Storage('lddb', app.config['DBNAME'], app.config['DBHOST'],
        app.config['DBUSER'], app.config['DBPASSWORD'])

def _json_response(data):
    return json.dumps(data), 200, {'Content-Type': MIMETYPE_JSON, 'Link': context_link}


@app.route('/relation')
def load_by_relation():
    rel = request.args.get('rel')
    ref = request.args.get('ref')
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit and limit.isdigit():
        limit = int(limit)
    if offset and offset.isdigit():
        offset = int(offset)
    items = [x[1] for x in storage.load_by_relation(rel, ref, limit, offset)]
    return _json_response(items)

@app.route('/context.jsonld')
def jsonld_context():
    return send_file(context_file, mimetype=MIMETYPE_JSONLD)

@app.route('/favicon.ico')
def favicon():
    abort(404)

@app.route('/<path:record_id>')
def load_record(record_id):
    result = storage.load("/"+record_id)
    if result:
        return _json_response(result[1])
    result = storage.load_thing("/"+record_id)
    if result:
        return redirect(result[0], 303)
    else:
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
