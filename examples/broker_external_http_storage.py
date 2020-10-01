# FLASK_APP=examples/broker_external_http_storage.py FLASK_RUN_PORT=9999 flask run

import json

from flask import Flask, request, jsonify, abort


app = Flask(__name__)

STORE_PATH = '/api/v1/store'

version = 0
store = None


@app.route(STORE_PATH, methods=['GET'])
def get():
    response = {
        'version': str(version) if version is not None else None,
        'store': store,
    }
    return jsonify(response)


@app.route(STORE_PATH, methods=['PUT'])
def update():
    content = request.get_json()
    global version, store
    v = content['version']
    s = content['store']

    if store is not None:
        if v is None:
            abort(400, 'version is empty')
        v = int(v)
        if v < version:
            abort(409, 'version conflict')
        if store == s:
            return ''

    if v is None:
        v = 0
    version = v + 1
    store = s
    return ''
