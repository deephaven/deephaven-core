#!/bin/bash

set -eu

if [ $# -ne 1 ]; then
  echo "Usage: $0 pageviews_per_second" 1>&2
  exit 1
fi

(echo "set pageviews_per_second $1"; echo quit) | nc localhost 8090
exit 0
