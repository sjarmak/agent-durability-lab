from __future__ import annotations

from pathlib import Path

import pytest

from temporal_native.fixture import FixtureRepository


def test_fixture_is_deterministic_and_independently_verifiable(tmp_path: Path) -> None:
    fixture = FixtureRepository.create(tmp_path / "fixture")

    before = fixture.snapshot()
    fixture.write("result.txt", "durable fixture\n")
    after = fixture.snapshot()

    assert before.files == {
        "README.md": "c69113d6d07b46bddd4f1aaaf0b9840639054c0c087e5ba70a7991cda873246c"
    }
    assert (
        after.files["result.txt"]
        == "d57ff0061e545cf742b3733692f16ab6d9347cfa3aaf7cff8382415643b346fa"
    )
    assert before.tree_sha256 != after.tree_sha256
    fixture.verify(after)


def test_fixture_rejects_path_escape(tmp_path: Path) -> None:
    fixture = FixtureRepository.create(tmp_path / "fixture")

    with pytest.raises(ValueError, match="relative path"):
        fixture.write("../outside.txt", "escape")
