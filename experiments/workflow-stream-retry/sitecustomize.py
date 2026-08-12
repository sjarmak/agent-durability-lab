"""Enable subprocess coverage only for an explicitly instrumented run."""

from __future__ import annotations

import os

if os.environ.get("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
