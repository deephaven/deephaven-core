#!/bin/bash

set -ex

exec docker-compose stop web envoy grpc-proxy server
