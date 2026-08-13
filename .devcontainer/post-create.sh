#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

require_version() {
  local actual="$1"
  local expected="$2"
  local tool="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'error: %s version is %q; expected %q\n' "${tool}" "${actual}" "${expected}" >&2
    return 1
  fi
}

require_version "$(go env GOVERSION)" "go1.25.12" "Go"
require_version "$(uv --version | awk '{print $2}')" "0.11.2" "uv"
require_version "$(python3.12 --version)" "Python 3.12.12" "Python"

(
  cd "${REPOSITORY_ROOT}"
  env -i \
    PATH="${PATH}" \
    HOME="${HOME}" \
    GOCACHE="${GOCACHE}" \
    GOMODCACHE="${GOMODCACHE}" \
    GOPATH="${GOPATH}" \
    GOENV=off \
    GOPRIVATE= \
    GONOSUMDB= \
    GOPROXY=https://proxy.golang.org \
    GOSUMDB=sum.golang.org \
    GOTOOLCHAIN=local \
    go mod download
)

printf '%s\n' \
  "Agent Durability Lab workspace is ready." \
  "Run: ./cookbooks/coding-agents/quickstart.sh" \
  "Smoke: ./cookbooks/coding-agents/dev-smoke.sh"
