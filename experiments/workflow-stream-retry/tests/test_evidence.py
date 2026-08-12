from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from workflow_stream_retry.barrier import BarrierArrival
from workflow_stream_retry.contract import (
    PublishedBatch,
    Scenario,
    StreamEvent,
    StreamObservation,
    TrialRecord,
    TrialVerdict,
)
from workflow_stream_retry.evidence import (
    TEMPORAL_CLI_VERSION,
    ProcessReceipt,
    TrialEvidence,
    _file_digest,
    _jsonable,
    _load_json,
    _open_directory,
    _read_regular_at,
    _trial_from_json,
    _utc,
    _validate_environment,
    source_pins,
)
from workflow_stream_retry.run_experiment import _validate_output


def _evidence() -> TrialEvidence:
    worker = ProcessReceipt("worker-1", 123, -15)
    event = StreamEvent("workflow-1/output", "complete", 1, worker.identity)
    record = TrialRecord(
        scenario=Scenario.UNFAULTED,
        trial=1,
        workflow_id="workflow-1",
        run_id="run-1",
        expected_output="ABC",
        final_attempt=1,
        final_worker_id=worker.identity,
        acknowledged_offset=0,
        observations=(StreamObservation(0, event),),
        batches=(PublishedBatch("publisher-1", 1, 1, (0,)),),
    )
    return TrialEvidence(
        record,
        TrialVerdict(True, "ABC", "ABC", False),
        (worker,),
        None,
        "2026-08-12T10:00:00Z",
        None,
        None,
        "2026-08-12T10:00:01Z",
    )


def test_trial_evidence_round_trip_is_closed_and_duplicate_safe() -> None:
    evidence = _evidence()
    encoded = json.dumps(_jsonable(evidence)).encode()
    assert _trial_from_json(encoded) == evidence
    with pytest.raises(ValueError):
        _trial_from_json(encoded.replace(b'"record":', b'"unknown":0,"record":', 1))
    with pytest.raises(ValueError):
        _load_json(b'{"schema":"one","schema":"two"}')
    with pytest.raises(ValueError, match="scalar type"):
        _trial_from_json(encoded.replace(b'"sequence": 1', b'"sequence": true'))
    with pytest.raises(ValueError, match="UTF-8"):
        _load_json('{"schema":"one"}'.encode("utf-16"))


def test_evidence_timestamps_require_utc() -> None:
    assert _utc("2026-08-12T10:00:00Z").utcoffset() is not None
    with pytest.raises(ValueError, match="UTC"):
        _utc("2026-08-12T10:00:00+01:00")


def test_environment_provenance_binds_current_runtime(tmp_path: Path) -> None:
    project_root = Path(__file__).parents[1]
    root = tmp_path / "run-1"
    root.mkdir()
    from workflow_stream_retry.evidence import EnvironmentRecord

    environment = EnvironmentRecord(
        captured_at="2026-08-12T10:00:00Z",
        temporalio_version=__import__("temporalio").__version__,
        temporal_cli=TEMPORAL_CLI_VERSION,
        temporal_cli_path=str(Path(shutil.which("temporal") or "").resolve()),
        temporal_cli_sha256=_file_digest(Path(shutil.which("temporal") or "").resolve())[0],
        python_version=platform.python_version(),
        python_executable_sha256=_file_digest(Path(sys.executable).resolve())[0],
        os=platform.system().lower(),
        architecture=platform.machine(),
        run_label=root.name,
        source_pins=source_pins(project_root),
    )
    _validate_environment(environment, root, project_root)
    for mutated in (
        replace(environment, python_version="attacker"),
        replace(environment, python_executable_sha256="00" * 32),
        replace(environment, os="attacker"),
        replace(environment, architecture="attacker"),
        replace(environment, temporal_cli="attacker"),
        replace(environment, temporal_cli_sha256="00" * 32),
    ):
        with pytest.raises(ValueError, match="provenance"):
            _validate_environment(mutated, root, project_root)


def test_pinned_directory_descriptor_ignores_path_replacement(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "value.json").write_text("original")
    directory = _open_directory(root)
    try:
        root.rename(tmp_path / "old-root")
        root.mkdir()
        (root / "value.json").write_text("attacker")
        assert _read_regular_at(directory, "value.json") == b"original"
    finally:
        os.close(directory)


@pytest.mark.parametrize(
    "path",
    [Path("/absolute"), Path("../escape"), Path("safe/../escape"), Path(r"safe\escape")],
)
def test_output_root_rejects_unconfined_paths(path: Path) -> None:
    with pytest.raises(ValueError):
        _validate_output(path)


def test_output_root_rejects_symlinked_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        _validate_output(Path("alias/evidence"))


def test_source_pins_cover_every_production_module() -> None:
    project_root = Path(__file__).parents[1]
    pins = source_pins(project_root)
    assert {pin.path for pin in pins} == {
        "pyproject.toml",
        "uv.lock",
        *{
            path.relative_to(project_root).as_posix()
            for path in (project_root / "workflow_stream_retry").glob("*.py")
        },
    }
    assert all(len(bytes.fromhex(pin.sha256)) == 32 and pin.bytes > 0 for pin in pins)


def test_fault_arrival_remains_structurally_serializable() -> None:
    arrival = BarrierArrival("post-flush-duplicate", "workflow-1", 1, "worker-1/pid-1")
    assert _jsonable(arrival) == {
        "point": "post-flush-duplicate",
        "workflow_id": "workflow-1",
        "attempt": 1,
        "worker_id": "worker-1/pid-1",
    }
