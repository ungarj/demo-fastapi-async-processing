#!/bin/bash

echo "start task:"
echo `curl -X 'GET' 'http://localhost/start/herbert' -H 'accept: application/json' -s`
echo ""
sleep 1

echo "get task status:"
watch echo `curl -X 'GET' 'http://localhost/status/herbert' -H 'accept: application/json' -s`
echo ""

# WAIT=$1
# echo "wait for ${WAIT} seconds and cancel task:"
# sleep ${WAIT}
# echo `curl -X 'GET' 'http://localhost/cancel/herbert' -H 'accept: application/json' -s`
# echo ""
# sleep 1

# echo "get task status:"
# echo `curl -X 'GET' 'http://localhost/status/herbert' -H 'accept: application/json' -s`
# echo ""