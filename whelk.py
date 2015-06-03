#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from lddb import Storage
from flask import Flask, abort, request

app = Flask(__name__)
app.config.from_pyfile('config.cfg')
app.secret_key = app.config.get('SESSION_SECRET_KEY')

storage = Storage('lddb', app.config['DBNAME'], app.config['DBHOST'], app.config['DBUSER'], app.config['DBPASSWORD'])

def _json_response(data):
    return json.dumps(data), 200, {'Content-Type':'application/json'}


@app.route('/relation')
def load_by_relation():
    relation = request.args.get('r')
    identifier = request.args.get('id')
    items = [x[1] for x in storage.load_by_relation(relation, identifier)]
    return _json_response(items)

@app.route('/<path:record_id>')
def load_record(record_id):
    item = storage.load("/"+record_id)
    if not item:
        abort(404)
    return _json_response(item[1])



if __name__ == '__main__':
    app.debug = True #app.config['DEBUG']
    app.run(host=app.config['BIND_HOST'], port=app.config['BIND_PORT'])

