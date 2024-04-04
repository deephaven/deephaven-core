#!/bin/bash

set -eu

exec docker compose run -T mzcli -f /scripts/demo.sql
