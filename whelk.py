#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from lddb import Storage
from flask import Flask, request, abort, redirect


app = Flask(__name__)
app.config.from_pyfile('config.cfg')
app.secret_key = app.config.get('SESSION_SECRET_KEY')

storage = Storage('lddb', app.config['DBNAME'], app.config['DBHOST'],
        app.config['DBUSER'], app.config['DBPASSWORD'])

def _json_response(data):
    return json.dumps(data), 200, {'Content-Type':'application/json'}


@app.route('/relation')
def load_by_relation():
    rel = request.args.get('rel')
    ref = request.args.get('ref')
    items = [x[1] for x in storage.load_by_relation(rel, ref)]
    return _json_response(items)

@app.route('/<path:record_id>')
def load_record(record_id):
    item = storage.load("/"+record_id)
    if not item:
        item = storage.load_thing("/"+record_id)
        return redirect(item[0], 303)
    if not item:
        abort(404)
    return _json_response(item[1])

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
