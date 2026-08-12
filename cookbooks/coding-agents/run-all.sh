#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  printf '%s\n' \
    "Usage: $0 {check|critical|all}" \
    "" \
    "  check     Read-only contract and admitted-evidence audits for all recipes." \
    "  critical  Focused integration, negative-control, mechanism, and replay gates." \
    "  all       Run check followed by critical."
}

check_suite() {
  COOKBOOK_SUITE_CHECK_MODE=1 PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "${SCRIPT_DIR}/tests" \
    -p 'test_suite.py' \
    -v
  "${SCRIPT_DIR}/01-native-agent-loop/run.sh" check
  (
    cd "${REPOSITORY_ROOT}"
    go test -race ./cookbooks/coding-agents/02-effect-safe-tools -count=1
    go run ./cookbooks/coding-agents/02-effect-safe-tools audit
  )
  "${SCRIPT_DIR}/03-external-cli-ownership/run.sh" check
  "${SCRIPT_DIR}/04-cancellation-and-cleanup/run.sh" check
  "${SCRIPT_DIR}/05-sandbox-lifecycle/run.sh" audit
  "${SCRIPT_DIR}/06-bounded-recovery/run.sh" check
}

run_critical_paths() {
  "${SCRIPT_DIR}/01-native-agent-loop/run.sh" critical
  (
    cd "${REPOSITORY_ROOT}"
    go test -race ./experiments/external-effects/internal/lab \
      -run 'TestProtectedDestinationsRejectConflictingPayloadWithoutMutation|TestProtectedGitRejectsConflictingMarker' \
      -count=1
  )
  "${SCRIPT_DIR}/03-external-cli-ownership/run.sh" critical
  "${SCRIPT_DIR}/04-cancellation-and-cleanup/run.sh" mechanisms
  "${SCRIPT_DIR}/05-sandbox-lifecycle/run.sh" critical
  "${SCRIPT_DIR}/06-bounded-recovery/run.sh" critical
}

case "${1:-}" in
  check)
    check_suite
    ;;
  critical)
    run_critical_paths
    ;;
  all)
    check_suite
    run_critical_paths
    ;;
  --help|-h)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
