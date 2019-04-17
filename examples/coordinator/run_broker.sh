#!/usr/bin/env bash
PORT=$1
cd /broker
pip install -r requirements.txt
echo "running on port" ${PORT}
FLASK_APP=simple_broker.py flask run --host=0.0.0.0 --port=${PORT}
