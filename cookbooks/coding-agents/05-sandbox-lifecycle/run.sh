#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly EXPERIMENT_ROOT="${REPOSITORY_ROOT}/experiments/durable-vendor-sessions/sandbox-harness"
readonly PROVIDER_TESTS='^(TestPinnedSandboxWorkflowDeduplicatesOuterUpdateButNotProviderEffect|TestPinnedUpstreamStartActivitySeparatesUpdateFromProviderIdempotency|TestStoreDistinguishesUnsafeAndIdempotentCommandDelivery|TestStoreRestoresExactSnapshotPrefix|TestStoreRejectsStaleAttachedWriterInFencedMode)$'
readonly LAB_TESTS='^(TestReconcileActiveInstancesAndExclusiveWrites|TestCurrentWorkflowsReplayCapturedParentCloseHistories)$'

usage() {
  printf '%s\n' \
    "Usage: $0 {audit|critical|all}" \
    "" \
    "  audit     Audit the cited v7 evidence and cookbook contract (read-only)." \
    "  critical  Run provider identity, fencing, snapshot, reconciliation, and replay tests." \
    "  all       Run audit followed by critical (the fresh-checkout path)."
}

audit_evidence() {
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "${SCRIPT_DIR}/tests" \
    -v
}

run_critical_path() {
  if ! command -v go >/dev/null 2>&1; then
    printf '%s\n' "error: Go is required to run the pinned sandbox experiment" >&2
    return 127
  fi

  (
    cd "${EXPERIMENT_ROOT}"
    go test -race ./internal/provider -run "${PROVIDER_TESTS}" -count=1
    go test -race ./internal/lab -run "${LAB_TESTS}" -count=1
  )
}

case "${1:-}" in
  audit)
    audit_evidence
    ;;
  critical)
    run_critical_path
    ;;
  all)
    audit_evidence
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
