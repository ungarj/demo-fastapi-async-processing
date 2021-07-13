#!/bin/bash

echo "start task:"
echo `curl -X 'GET' 'http://localhost/start/herbert' -H 'accept: application/json'`
echo ""

echo "get task status:"
echo `curl -X 'GET' 'http://localhost/status/herbert' -H 'accept: application/json'`
echo ""

echo "cancel task:"
echo `curl -X 'GET' 'http://localhost/cancel/herbert' -H 'accept: application/json'`
echo ""

echo "get task status:"
echo `curl -X 'GET' 'http://localhost/status/herbert' -H 'accept: application/json'`
echo ""