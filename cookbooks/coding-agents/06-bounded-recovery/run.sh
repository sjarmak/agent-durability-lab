#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly EVIDENCE_ROOT="${REPOSITORY_ROOT}/benchmarks/agent-durability/topology/evidence/recovery-20260811-v7"

usage() {
  printf '%s\n' \
    "Usage: $0 {check|critical|all}" \
    "" \
    "  check     Verify cookbook contract and sealed v7 inventory (read-only)." \
    "  critical  Recompute every run verdict and replay every stored history." \
    "  all       Run check followed by critical."
}

check_cookbook() {
  COOKBOOK_CHECK_MODE=1 PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "${SCRIPT_DIR}/tests" \
    -p 'test_cookbook.py' \
    -v
}

run_critical_path() {
  (
    cd "${REPOSITORY_ROOT}"
    TOPOLOGY_RECOVERY_EVIDENCE_ROOT="${EVIDENCE_ROOT}" \
      go test -race ./benchmarks/agent-durability/topology/semantics \
      -run '^TestPreservedRecoveryEvidenceAuditsFromDisk$' \
      -count=1
  )
}

case "${1:-}" in
  check)
    check_cookbook
    ;;
  critical)
    run_critical_path
    ;;
  all)
    check_cookbook
    run_critical_path
    ;;
  --help|-h)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
