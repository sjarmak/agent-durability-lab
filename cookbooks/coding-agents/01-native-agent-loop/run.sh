#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly EXPERIMENT_ROOT="${REPOSITORY_ROOT}/experiments/durable-vendor-sessions/temporal-native"
readonly CRITICAL_TEST="tests/test_workflow.py::test_agent_loop_correlates_model_tool_destination_and_result"

usage() {
  printf '%s\n' \
    "Usage: $0 {check|critical|all}" \
    "" \
    "  check     Verify cookbook citations, commands, and admitted evidence (read-only)." \
    "  critical  Run the typed-result integration path and replay its Temporal history." \
    "  all       Run check followed by critical (the fresh-checkout path)."
}

check_cookbook() {
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
    -s "${SCRIPT_DIR}/tests" \
    -v
}

run_critical_path() {
  if ! command -v uv >/dev/null 2>&1; then
    printf '%s\n' "error: uv is required to run the pinned experiment" >&2
    return 127
  fi

  (
    cd "${EXPERIMENT_ROOT}"
    PYTHONDONTWRITEBYTECODE=1 uv run --locked pytest \
      -p no:cacheprovider \
      -q \
      "${CRITICAL_TEST}"
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
