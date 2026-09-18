#!/usr/bin/env sh
set -eu

mkdir -p test-results

unformatted="$(gofmt -l cmd internal)"
if [ -n "$unformatted" ]; then
  echo "gofmt found unformatted files:" >&2
  echo "$unformatted" >&2
  exit 1
fi

go test -json ./... > test-results/go.json 2>&1
status=$?

exit "$status"
