#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly AUDIT_PACKAGE="./experiments/durable-vendor-sessions/codex-direct/internal/lab"
readonly AUDIT_TEST='^TestAdmittedTransportsReconstructEveryVerdict$'

if (( $# != 0 )); then
  printf '%s\n' "usage: $0" >&2
  exit 2
fi
if ! command -v go >/dev/null 2>&1; then
  printf '%s\n' "error: Go is required for the credential-free quickstart" >&2
  exit 127
fi

GO_BINARY="$(command -v go)"
GO_MODULE_CACHE="$(env -i PATH="${PATH}" HOME="${HOME}" GOENV=off "${GO_BINARY}" env GOMODCACHE)"
SANDBOX_ROOT="$(mktemp -d /tmp/coding-agent-quickstart.XXXXXX)"
readonly GO_BINARY GO_MODULE_CACHE SANDBOX_ROOT
readonly RECEIPTS="${SANDBOX_ROOT}/receipts.jsonl"
mkdir -p "${SANDBOX_ROOT}/cache" "${SANDBOX_ROOT}/home" "${SANDBOX_ROOT}/tmp"
cleanup() {
  rm -rf -- "${SANDBOX_ROOT}"
}
trap cleanup EXIT

if ! (
  cd "${REPOSITORY_ROOT}"
  env -i \
    PATH="${PATH}" \
    HOME="${SANDBOX_ROOT}/home" \
    TMPDIR="${SANDBOX_ROOT}/tmp" \
    GOCACHE="${SANDBOX_ROOT}/cache" \
    GOMODCACHE="${GO_MODULE_CACHE}" \
    GOENV=off \
    GOFLAGS= \
    CODEX_DIRECT_TRANSPORT_AUDIT=1 \
    "${GO_BINARY}" test -race -count=1 -json -timeout=2m \
      "${AUDIT_PACKAGE}" -run "${AUDIT_TEST}" >"${RECEIPTS}"
); then
  sed -n '1,240p' "${RECEIPTS}" >&2
  exit 1
fi

(
  cd "${REPOSITORY_ROOT}"
  env -i \
    PATH="${PATH}" \
    HOME="${SANDBOX_ROOT}/home" \
    TMPDIR="${SANDBOX_ROOT}/tmp" \
    GOCACHE="${SANDBOX_ROOT}/cache" \
    GOMODCACHE="${GO_MODULE_CACHE}" \
    GOENV=off \
    GOFLAGS= \
    "${GO_BINARY}" run ./cookbooks/coding-agents/quickstart/cmd/summary <"${RECEIPTS}"
)
