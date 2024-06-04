#!/usr/bin/env bash

set -e

docker build -t registry.apps.nickv.me/python-cdc-capture .
docker push registry.apps.nickv.me/python-cdc-capture
kubectl replace -f job.python-cdc-capture.yaml --force