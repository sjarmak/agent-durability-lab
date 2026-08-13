#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if (( $# != 0 )); then
  printf '%s\n' "usage: $0" >&2
  exit 2
fi
if ! command -v go >/dev/null 2>&1; then
  printf '%s\n' "error: Go is required for the evidence explorer" >&2
  exit 127
fi

cd "${REPOSITORY_ROOT}"
exec env -i \
  PATH="${PATH}" \
  HOME="${HOME}" \
  GOENV=off \
  GOFLAGS= \
  go run ./cookbooks/coding-agents/explorer/cmd/explorer \
    --repository "${REPOSITORY_ROOT}" \
    --listen 127.0.0.1:8080
