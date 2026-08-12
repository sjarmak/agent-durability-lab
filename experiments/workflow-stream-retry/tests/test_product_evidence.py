from __future__ import annotations

from pathlib import Path

import pytest
import temporalio

from workflow_stream_retry.product_evidence import (
    _require_candidate_patch,
    _require_python_version,
)


def test_product_evidence_requires_the_registered_python_version() -> None:
    _require_python_version("3.12.3")
    with pytest.raises(ValueError, match="Python version"):
        _require_python_version("3.11.15")


def test_product_evidence_binds_the_sdk_tree_to_the_patch(tmp_path: Path) -> None:
    sdk_root = Path(temporalio.__file__).resolve().parents[1]
    patch = (
        Path(__file__).parents[3]
        / "contrib"
        / "sdk-python-retry-aware-streams"
        / "sdk-python-d489a5d-retry-aware-streams.patch"
    )
    _require_candidate_patch(sdk_root, patch)

    corrupted = tmp_path / "candidate.patch"
    corrupted.write_bytes(patch.read_bytes() + b"# corruption\n")
    with pytest.raises(ValueError, match="working-tree diff"):
        _require_candidate_patch(sdk_root, corrupted)
