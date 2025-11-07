#!/bin/bash

set -eu

docker compose stop && docker compose down -v
