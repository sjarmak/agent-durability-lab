#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly LAB_PACKAGE="./experiments/durable-vendor-sessions/claude-direct/internal/lab"
readonly CRITICAL_TESTS='^(TestRunExperimentWithFakeClaudeProvesResumeOnlyDoesNotFenceEffects|TestRunExperimentWithFakeClaudeFencedSupervisorAttachesOnce|TestSupervisorConcurrencySensitiveScenariosRepeatThreeTrials)$'
readonly TRANSPORT_COMMAND="./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport"
readonly AUDIT_COMMAND="./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-audit"
readonly CODEX_LAB_PACKAGE="./experiments/durable-vendor-sessions/codex-direct/internal/lab"
readonly CODEX_AUDIT_TEST='^TestAdmittedTransportsReconstructEveryVerdict$'
readonly DIRECT_TRANSPORT="${REPOSITORY_ROOT}/experiments/durable-vendor-sessions/claude-direct/evidence-transport"
readonly RESUME_TRANSPORT="${REPOSITORY_ROOT}/experiments/durable-vendor-sessions/claude-direct/resume-evidence-transport"
readonly FENCED_TRANSPORT="${REPOSITORY_ROOT}/experiments/durable-vendor-sessions/claude-direct/fenced-evidence-transport-v2"

usage() {
  printf '%s\n' \
    "Usage: $0 {check|critical|all}" \
    "" \
    "  check     Verify citations, sealed evidence hashes, and verdicts (read-only)." \
    "  critical  Run hermetic resume, fenced supervisor, and repeated mechanism tests." \
    "  all       Run check followed by critical."
}

check_cookbook() (
  local restore_parent
  restore_parent="$(mktemp -d /tmp/coding-agent-cli-cookbook.XXXXXX)"
  cleanup_restore() {
    rm -rf -- "${restore_parent}"
  }
  trap cleanup_restore EXIT

  (
    cd "${REPOSITORY_ROOT}"
    for specification in \
      "${DIRECT_TRANSPORT}:${restore_parent}/direct" \
      "${RESUME_TRANSPORT}:${restore_parent}/resume" \
      "${FENCED_TRANSPORT}:${restore_parent}/fenced"
    do
      local transport="${specification%%:*}"
      local output="${specification#*:}"
      go run "${TRANSPORT_COMMAND}" verify --transport "${transport}"
      go run "${TRANSPORT_COMMAND}" restore \
        --transport "${transport}" \
        --output "${output}"
    done
    go run "${AUDIT_COMMAND}" \
      --mode direct \
      --root "${restore_parent}/direct/claude-direct-20260808-v5" \
      --output "${restore_parent}/direct-audit.json"
    go run "${AUDIT_COMMAND}" \
      --mode resume-only \
      --root "${restore_parent}/resume/claude-direct-resume-20260810-v5" \
      --output "${restore_parent}/resume-audit.json"
    go run "${AUDIT_COMMAND}" \
      --mode fenced \
      --root "${restore_parent}/fenced/claude-direct-fenced-hermetic-20260811-v4" \
      --output "${restore_parent}/fenced-audit.json"

    local codex_output
    if ! codex_output="$(CODEX_DIRECT_TRANSPORT_AUDIT=1 \
      go test -race -count=1 -v "${CODEX_LAB_PACKAGE}" \
      -run "${CODEX_AUDIT_TEST}" 2>&1)"; then
      printf '%s\n' "${codex_output}"
      return 1
    fi
    printf '%s\n' "${codex_output}"
    if grep -Eq -- '--- SKIP|no tests to run' <<<"${codex_output}"; then
      printf '%s\n' "error: the Codex transport audit was skipped" >&2
      return 1
    fi
  )

  COOKBOOK_CHECK_MODE=1 \
    COOKBOOK_DIRECT_ROOT="${restore_parent}/direct/claude-direct-20260808-v5" \
    COOKBOOK_RESUME_ROOT="${restore_parent}/resume/claude-direct-resume-20260810-v5" \
    COOKBOOK_FENCED_ROOT="${restore_parent}/fenced/claude-direct-fenced-hermetic-20260811-v4" \
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
      -s "${SCRIPT_DIR}/tests" \
      -p 'test_cookbook.py' \
      -v
)

run_critical_path() {
  if ! command -v go >/dev/null 2>&1; then
    printf '%s\n' "error: Go is required for the external-CLI critical path" >&2
    return 127
  fi
  if ! command -v temporal >/dev/null 2>&1; then
    printf '%s\n' "error: the pinned Temporal CLI must be on PATH" >&2
    return 127
  fi
  (
    cd "${REPOSITORY_ROOT}"
    local output
    if ! output="$(go test -race -v "${LAB_PACKAGE}" -run "${CRITICAL_TESTS}" -count=1 2>&1)"; then
      printf '%s\n' "${output}"
      return 1
    fi
    printf '%s\n' "${output}"
    if grep -Eq -- '--- SKIP|no tests to run' <<<"${output}"; then
      printf '%s\n' "error: an advertised external-CLI critical test was skipped" >&2
      return 1
    fi
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
