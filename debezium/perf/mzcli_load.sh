#!/bin/bash

set -ex

exec docker-compose run -T mzcli -f /scripts/demo.sql
