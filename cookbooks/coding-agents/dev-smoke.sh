#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if (( $# != 0 )); then
  printf '%s\n' "usage: $0" >&2
  exit 2
fi

for command in go python3.12 setsid; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    printf 'error: %s is required for the development smoke gate\n' "${command}" >&2
    exit 127
  fi
done

"${SCRIPT_DIR}/quickstart.sh"

(
  explorer_log="$(mktemp /tmp/coding-agent-explorer-smoke.XXXXXX)"
  explorer_pid=
  cleanup_explorer() {
    if [[ -n "${explorer_pid}" ]] && kill -0 -- "-${explorer_pid}" 2>/dev/null; then
      kill -TERM -- "-${explorer_pid}" 2>/dev/null || true
      wait "${explorer_pid}" 2>/dev/null || true
    fi
    rm -f "${explorer_log}"
  }
  trap cleanup_explorer EXIT HUP INT TERM
  setsid "${SCRIPT_DIR}/explore.sh" >"${explorer_log}" 2>&1 &
  explorer_pid=$!
  python3.12 - <<'PY'
import json
import time
import urllib.request

deadline = time.monotonic() + 20
while True:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/api/catalog", timeout=1) as response:
            catalog = json.load(response)
            if response.status != 200 or catalog.get("schema_version") != "coding-agent-presentation-v1":
                raise RuntimeError("explorer catalog response is invalid")
            break
    except Exception:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.05)
PY
  kill -TERM -- "-${explorer_pid}"
  explorer_status=0
  wait "${explorer_pid}" || explorer_status=$?
  if [[ "${explorer_status}" -ne 0 && "${explorer_status}" -ne 143 ]]; then
    cat "${explorer_log}" >&2
    printf 'error: evidence explorer exited with status %s\n' "${explorer_status}" >&2
    exit 1
  fi
  explorer_group_stopped=false
  for _ in $(seq 1 100); do
    if ! kill -0 -- "-${explorer_pid}" 2>/dev/null; then
      explorer_group_stopped=true
      break
    fi
    sleep 0.05
  done
  if [[ "${explorer_group_stopped}" != true ]]; then
    kill -KILL -- "-${explorer_pid}" 2>/dev/null || true
    printf '%s\n' "error: evidence explorer process group survived shutdown" >&2
    exit 1
  fi
  explorer_pid=
  grep -F "Recovery evidence explorer: http://127.0.0.1:8080" "${explorer_log}" >/dev/null
)

(
  cd "${REPOSITORY_ROOT}"
  GOENV=off go test -race -count=1 \
    ./cookbooks/coding-agents/presentation/... \
    ./cookbooks/coding-agents/quickstart/... \
    ./cookbooks/coding-agents/explorer/...
  COOKBOOK_SUITE_CHECK_MODE=1 python3.12 -m unittest \
    cookbooks.coding-agents.tests.test_suite
)
