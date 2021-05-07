#!/bin/bash

set -e

########################################
## GitHub Runner Installation
## Deephaven
########################################

GH_RUNNER_VERSION=${GH_RUNNER_VERSION:="2.277.1"}
GH_RUNNER_REPO_URL=${GH_RUNNER_REPO_URL:="https://github.com/deephaven/core"}
GH_RUNNER_TOKEN=${GH_RUNNER_TOKEN:?""}
GH_RUNNER_NAME=${GH_RUNNER_NAME:=$HOSTNAME}
GH_RUNNER_LABELS=${GH_RUNNER_LABELS:="gce-runner,benchmark"}
GH_RUNNER_ROOT=${GH_RUNNER_ROOT:="/github"}
GH_RUNNER_TMP=${GH_RUNNER_TMP:="/github-tmp"}

## Prepare Apt system packages
apt update
apt install --yes curl unzip docker.io

## JDK Build Prereq
apt install --yes openjdk-8-jdk-headless

## Setup runner directories
mkdir -p $GH_RUNNER_ROOT
mkdir -p $GH_RUNNER_TMP

## Pull runner binary
curl -o actions.tar.gz --location "https://github.com/actions/runner/releases/download/v${GH_RUNNER_VERSION}/actions-runner-linux-x64-${GH_RUNNER_VERSION}.tar.gz"
tar xzf actions.tar.gz --directory $GH_RUNNER_ROOT
rm -f actions.tar.gz

## Install dependencies
/github/bin/installdependencies.sh

## Configure runner
RUNNER_ALLOW_RUNASROOT=1 $GH_RUNNER_ROOT/config.sh --unattended --replace --work $GH_RUNNER_TMP --url "$GH_RUNNER_REPO_URL" --token "$GH_RUNNER_TOKEN" --labels $GH_RUNNER_LABELS

## Configure runner as service
cd $GH_RUNNER_ROOT || exit
./svc.sh install
./svc.sh start