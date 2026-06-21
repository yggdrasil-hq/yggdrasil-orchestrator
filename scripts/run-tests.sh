#!/usr/bin/env sh
set -eu

mkdir -p test-results

go test -json ./... > test-results/go.json 2>&1
status=$?

exit "$status"
