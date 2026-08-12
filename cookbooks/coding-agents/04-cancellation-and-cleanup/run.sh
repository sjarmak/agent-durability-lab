#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

usage() {
  printf '%s\n' \
    "Usage: $0 {check|mechanisms|all}" \
    "" \
    "  check       Audit cookbook citations and admitted v2 evidence (read-only)." \
    "  mechanisms  Run store and process-control negative/protected tests." \
    "  all         Run check followed by mechanisms."
}

check_cookbook() {
  COOKBOOK_CHECK_MODE=1 PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "${SCRIPT_DIR}/tests" \
    -p 'test_cookbook.py' \
    -v
}

run_mechanisms() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    printf '%s\n' "error: process identity and process-tree controls require Linux" >&2
    return 1
  fi
  (
    cd "${REPOSITORY_ROOT}"
    go test -race ./internal/workstore -run 'TestCancellation' -count=1
    local output
    if ! output="$(go test -race -v ./internal/agentprocess \
      -run 'TestSignalLeaderOnly|TestSignalProcessTree|TestDelayedStaleStop' \
      -count=1 2>&1)"; then
      printf '%s\n' "${output}"
      return 1
    fi
    printf '%s\n' "${output}"
    if grep -Eq -- '--- SKIP|no tests to run' <<<"${output}"; then
      printf '%s\n' "error: an advertised process-control test was skipped" >&2
      return 1
    fi
  )
}

case "${1:-}" in
  check)
    check_cookbook
    ;;
  mechanisms)
    run_mechanisms
    ;;
  all)
    check_cookbook
    run_mechanisms
    ;;
  --help|-h)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
