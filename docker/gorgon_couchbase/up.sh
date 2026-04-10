#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

cd $(dirname "$0")
pwd

CB_VERSION=${CB_VERSION:-}
make CB_VERSION=$CB_VERSION

docker compose -f compose.yaml up --force-recreate $*
