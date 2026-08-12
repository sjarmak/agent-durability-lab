from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spec_root() -> Path:
    return Path(__file__).resolve().parents[4] / "specs" / "coding-agent-durability" / "v1"
