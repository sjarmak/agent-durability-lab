from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import platform
import shutil
import stat
import sys
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from temporalio import __version__ as temporalio_version
from temporalio.api.enums.v1 import TimeoutType
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from .barrier import BarrierArrival
from .contract import (
    ActivityResult,
    PublishedBatch,
    Scenario,
    StreamEvent,
    StreamObservation,
    TrialRecord,
    TrialVerdict,
    WorkflowInput,
    WorkflowResult,
)
from .history import (
    activity_attempts,
    activity_result,
    activity_retry_failures,
    batch_actors,
    inspect_stream_history,
    workflow_input,
    workflow_result,
)
from .oracle import _validate_types, audit_trial
from .runner import TrialCapture
from .workflow import StreamRetryWorkflow

SCHEMA = "workflow-stream-retry-evidence-v1"
TEMPORAL_CLI_VERSION = "temporal version 1.8.0 (Server 1.31.2, UI 2.50.1)"
MAX_JSON_BYTES = 16 << 20
MAX_JSON_DEPTH = 64
MAX_JSON_ITEMS = 10_000
SOURCE_FILES = ("pyproject.toml", "uv.lock")


@dataclass(frozen=True)
class SourcePin:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class EnvironmentRecord:
    captured_at: str
    temporalio_version: str
    temporal_cli: str
    temporal_cli_path: str
    temporal_cli_sha256: str
    python_version: str
    python_executable_sha256: str
    os: str
    architecture: str
    run_label: str
    source_pins: tuple[SourcePin, ...]


@dataclass(frozen=True)
class ProcessReceipt:
    worker_id: str
    pid: int
    exit_code: int

    @property
    def identity(self) -> str:
        return f"{self.worker_id}/pid-{self.pid}"


@dataclass(frozen=True)
class TrialEvidence:
    record: TrialRecord
    verdict: TrialVerdict
    worker_processes: tuple[ProcessReceipt, ...]
    fault_arrival: BarrierArrival | None
    started_at: str
    fault_at: str | None
    replacement_at: str | None
    completed_at: str


@dataclass(frozen=True)
class ExperimentReport:
    schema: str
    environment: EnvironmentRecord
    trials: tuple[TrialEvidence, ...]


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class EvidenceManifest:
    schema: str
    entries: tuple[ManifestEntry, ...]


def trial_evidence(capture: TrialCapture) -> TrialEvidence:
    return TrialEvidence(
        record=capture.record,
        verdict=capture.verdict,
        worker_processes=tuple(ProcessReceipt(*process) for process in capture.worker_processes),
        fault_arrival=capture.fault_arrival,
        started_at=capture.started_at,
        fault_at=capture.fault_at,
        replacement_at=capture.replacement_at,
        completed_at=capture.completed_at,
    )


def build_environment(
    project_root: Path,
    run_label: str,
    temporal_cli: str,
    temporal_cli_path: Path,
) -> EnvironmentRecord:
    if temporal_cli.strip() != TEMPORAL_CLI_VERSION:
        raise ValueError("Temporal CLI version differs from the pinned experiment")
    resolved_temporal = temporal_cli_path.resolve(strict=True)
    return EnvironmentRecord(
        captured_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        temporalio_version=temporalio_version,
        temporal_cli=temporal_cli.strip(),
        temporal_cli_path=str(resolved_temporal),
        temporal_cli_sha256=_file_digest(resolved_temporal)[0],
        python_version=platform.python_version(),
        python_executable_sha256=_file_digest(Path(sys.executable).resolve(strict=True))[0],
        os=platform.system().lower(),
        architecture=platform.machine(),
        run_label=run_label,
        source_pins=source_pins(project_root),
    )


def source_pins(project_root: Path) -> tuple[SourcePin, ...]:
    paths = [project_root / name for name in SOURCE_FILES]
    paths.extend(sorted((project_root / "workflow_stream_retry").glob("*.py")))
    pins: list[SourcePin] = []
    for path in paths:
        digest, size = _file_digest(path)
        pins.append(SourcePin(path.relative_to(project_root).as_posix(), digest, size))
    return tuple(pins)


def preserve_trial(root: Path, capture: TrialCapture) -> TrialEvidence:
    evidence = trial_evidence(capture)
    prefix = _prefix(capture.record)
    _write_exclusive(root / f"{prefix}-history.json", capture.history.to_json().encode() + b"\n")
    _write_json_exclusive(root / f"{prefix}-trial.json", evidence)
    return evidence


def preserve_report(root: Path, report: ExperimentReport) -> None:
    _write_json_exclusive(root / "report.json", report)
    entries = _inventory(root, exclude_manifest=True)
    _write_json_exclusive(root / "manifest.json", EvidenceManifest(SCHEMA, entries))


def preserve_failure(root: Path, error: BaseException) -> None:
    with suppress(FileExistsError):
        _write_json_exclusive(root / "failure.json", {"schema": SCHEMA, "error": str(error)})


async def audit_evidence(root: Path, project_root: Path) -> ExperimentReport:
    _reject_symlink_components(root)
    directory = _open_directory(root)
    try:
        manifest = _manifest_from_json(_read_regular_at(directory, "manifest.json"))
        if manifest.schema != SCHEMA:
            raise ValueError("evidence manifest schema differs")
        _validate_inventory(manifest.entries)
        bundle = _read_bundle_at(directory, manifest.entries)
        report = _report_from_json(bundle["report.json"])
        _validate_environment(report.environment, root, project_root)
        await _audit_trials(bundle, report)
        return report
    finally:
        os.close(directory)


async def _audit_trials(bundle: dict[str, bytes], report: ExperimentReport) -> None:
    expected_schedule = tuple((scenario, trial) for scenario in Scenario for trial in range(1, 4))
    if report.schema != SCHEMA or tuple(
        (trial.record.scenario, trial.record.trial) for trial in report.trials
    ) != expected_schedule:
        raise ValueError("evidence report schedule differs")
    for stored in report.trials:
        expected_workflow_id = (
            f"{report.environment.run_label}-{stored.record.scenario.value}"
            f"-trial-{stored.record.trial}"
        )
        if stored.record.workflow_id != expected_workflow_id:
            raise ValueError("Workflow identity differs from the experiment schedule")
        prefix = _prefix(stored.record)
        trial = _trial_from_json(bundle[f"{prefix}-trial.json"])
        if trial != stored:
            raise ValueError("stored trial views differ")
        history = WorkflowHistory.from_json(
            trial.record.workflow_id,
            bundle[f"{prefix}-history.json"].decode(),
        )
        _audit_history(trial, history, report.environment.captured_at)
        await Replayer(workflows=[StreamRetryWorkflow]).replay_workflow(history)


def _audit_history(trial: TrialEvidence, history: WorkflowHistory, captured_at: str) -> None:
    if history.run_id != trial.record.run_id:
        raise ValueError("Workflow history run identity differs")
    if workflow_input(history) != WorkflowInput(
        trial.record.scenario, trial.record.trial, "ABC"
    ):
        raise ValueError("Workflow history input differs")
    expected_result = WorkflowResult(
        trial.record.expected_output,
        trial.record.final_attempt,
        trial.record.final_worker_id,
        trial.record.acknowledged_offset,
    )
    if workflow_result(history) != expected_result:
        raise ValueError("Workflow history result differs")
    if activity_result(history) != ActivityResult(
        trial.record.expected_output, trial.record.final_attempt, trial.record.final_worker_id
    ):
        raise ValueError("Activity history result differs")
    batches, observations = inspect_stream_history(history)
    if batches != trial.record.batches or observations != trial.record.observations:
        raise ValueError("history stream publications differ from consumer evidence")
    if audit_trial(trial.record) != trial.verdict:
        raise ValueError("stored trial verdict differs")
    _validate_processes(trial, history, captured_at)


def _reject_symlink_components(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError("evidence path contains a symlink")


def _validate_environment(
    environment: EnvironmentRecord,
    root: Path,
    project_root: Path,
) -> None:
    current_temporal = shutil.which("temporal")
    try:
        captured = datetime.fromisoformat(environment.captured_at.replace("Z", "+00:00"))
        executable_digest = bytes.fromhex(environment.python_executable_sha256)
    except ValueError as error:
        raise ValueError("environment provenance is malformed") from error
    if (
        captured.tzinfo is None
        or captured.utcoffset() != timedelta(0)
        or environment.temporalio_version != temporalio_version
        or environment.temporal_cli != TEMPORAL_CLI_VERSION
        or current_temporal is None
        or Path(environment.temporal_cli_path) != Path(current_temporal).resolve(strict=True)
        or environment.temporal_cli_sha256
        != _file_digest(Path(environment.temporal_cli_path))[0]
        or environment.python_version != platform.python_version()
        or len(executable_digest) != 32
        or environment.python_executable_sha256
        != _file_digest(Path(sys.executable).resolve(strict=True))[0]
        or environment.os != platform.system().lower()
        or environment.architecture != platform.machine()
        or environment.run_label != root.name
        or environment.source_pins != source_pins(project_root)
    ):
        raise ValueError("environment provenance differs")


def _validate_processes(
    trial: TrialEvidence,
    history: WorkflowHistory,
    captured_at: str,
) -> None:
    expected_count = 1 if trial.record.scenario is Scenario.UNFAULTED else 2
    if len(trial.worker_processes) != expected_count:
        raise ValueError("Worker process count differs")
    expected_workers = tuple(
        f"{trial.record.workflow_id}/worker-{index}"
        for index in range(1, expected_count + 1)
    )
    if (
        tuple(process.worker_id for process in trial.worker_processes) != expected_workers
        or any(process.pid <= 0 for process in trial.worker_processes)
    ):
        raise ValueError("Worker process identity differs")
    expected_attempts = (
        ((1, trial.worker_processes[0].identity),)
        if trial.record.scenario is Scenario.UNFAULTED
        else ((2, trial.worker_processes[1].identity),)
    )
    if activity_attempts(history) != expected_attempts:
        raise ValueError("Activity attempt process identities differ")
    expected_failure = (
        ((1, None),)
        if trial.record.scenario is Scenario.UNFAULTED
        else ((2, int(TimeoutType.TIMEOUT_TYPE_HEARTBEAT)),)
    )
    if activity_retry_failures(history) != expected_failure:
        raise ValueError("Activity retry failure differs")
    if trial.record.final_worker_id != trial.worker_processes[-1].identity:
        raise ValueError("final Worker process receipt differs")
    workers_by_attempt = {
        1: trial.worker_processes[0].identity,
        expected_count: trial.worker_processes[-1].identity,
    }
    if any(
        observation.event.worker_id != workers_by_attempt.get(observation.event.attempt)
        for observation in trial.record.observations
    ):
        raise ValueError("stream event Worker process identity differs")
    expected_actors = tuple(
        (batch.activity_attempt, trial.worker_processes[batch.activity_attempt - 1].pid)
        for batch in trial.record.batches
    )
    actual_actors = batch_actors(history)
    if len(actual_actors) != len(expected_actors) or any(
        actor.activity_attempt != attempt
        or not actor.identity.startswith(f"{pid}@")
        or len(actor.identity.split("@", 1)[1]) == 0
        for actor, (attempt, pid) in zip(actual_actors, expected_actors, strict=True)
    ):
        raise ValueError("stream publish actor differs from Worker process")
    if trial.worker_processes[-1].exit_code != -15:
        raise ValueError("replacement Worker did not exit by controlled termination")
    timestamps = [_utc(trial.started_at)]
    if trial.record.scenario is Scenario.UNFAULTED:
        if (
            trial.fault_arrival is not None
            or trial.fault_at is not None
            or trial.replacement_at is not None
        ):
            raise ValueError("unfaulted trial contains a barrier arrival")
    else:
        expected_arrival = BarrierArrival(
            trial.record.scenario.value,
            trial.record.workflow_id,
            1,
            trial.worker_processes[0].identity,
        )
        if trial.fault_arrival != expected_arrival:
            raise ValueError("fault barrier receipt differs")
        if trial.worker_processes[0].exit_code != -9:
            raise ValueError("faulted Worker was not killed")
        if trial.fault_at is None or trial.replacement_at is None:
            raise ValueError("fault timestamps are absent")
        timestamps.extend((_utc(trial.fault_at), _utc(trial.replacement_at)))
    timestamps.append(_utc(trial.completed_at))
    if timestamps != sorted(timestamps):
        raise ValueError("trial timestamps are not monotonic")
    history_times = tuple(event.event_time.ToDatetime(tzinfo=UTC) for event in history.events)
    if (
        not history_times
        or _utc(captured_at) > timestamps[0]
        or timestamps[0] > history_times[0]
        or history_times[-1] > timestamps[-1]
    ):
        raise ValueError("trial timestamps differ from Temporal Event History")


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp is not UTC")
    return parsed


def _validate_inventory(entries: tuple[ManifestEntry, ...]) -> None:
    expected = _expected_inventory()
    if {entry.path for entry in entries} != expected or len(entries) != len(expected):
        raise ValueError("evidence inventory is not exact")


def _expected_inventory() -> set[str]:
    expected = {"report.json"}
    for scenario in Scenario:
        for trial in range(1, 4):
            prefix = f"{scenario.value}-trial-{trial}"
            expected.update({f"{prefix}-history.json", f"{prefix}-trial.json"})
    return expected


def _inventory(root: Path, *, exclude_manifest: bool) -> tuple[ManifestEntry, ...]:
    directory = _open_directory(root)
    try:
        return _inventory_at(directory, exclude_manifest=exclude_manifest)
    finally:
        os.close(directory)


def _inventory_at(directory: int, *, exclude_manifest: bool) -> tuple[ManifestEntry, ...]:
    names = set(os.listdir(directory))
    if exclude_manifest:
        names.discard("manifest.json")
    if names != _expected_inventory():
        raise ValueError("evidence inventory is not exact")
    entries: list[ManifestEntry] = []
    for name in sorted(names):
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(f"evidence entry is not a regular file: {name}")
        if info.st_size > MAX_JSON_BYTES:
            raise ValueError(f"evidence entry is too large: {name}")
        digest, size = _file_digest_at(directory, name)
        entries.append(ManifestEntry(name, digest, size))
    return tuple(entries)


def _read_bundle_at(
    directory: int,
    manifest_entries: tuple[ManifestEntry, ...],
) -> dict[str, bytes]:
    names = set(os.listdir(directory))
    names.discard("manifest.json")
    entries = {entry.path: entry for entry in manifest_entries}
    if names != _expected_inventory() or set(entries) != names:
        raise ValueError("evidence inventory is not exact")
    bundle: dict[str, bytes] = {}
    for name in sorted(names):
        data = _read_regular_at(directory, name)
        entry = entries[name]
        if len(data) != entry.bytes or hashlib.sha256(data).hexdigest() != entry.sha256:
            raise ValueError(f"evidence entry digest differs: {name}")
        bundle[name] = data
    return bundle


def _file_digest(path: Path) -> tuple[str, int]:
    data = _read_regular(path, limit=None)
    return hashlib.sha256(data).hexdigest(), len(data)


def _read_regular(path: Path, *, limit: int | None = MAX_JSON_BYTES) -> bytes:
    directory = _open_directory(path.parent)
    try:
        return _read_regular_at(directory, path.name, limit=limit)
    finally:
        os.close(directory)


def _read_regular_at(
    directory: int,
    name: str,
    *,
    limit: int | None = MAX_JSON_BYTES,
) -> bytes:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(f"file is not regular: {name}")
        if limit is not None and info.st_size > limit:
            raise ValueError(f"file is too large: {name}")
        with os.fdopen(descriptor, "rb", closefd=False) as file:
            return file.read() if limit is None else file.read(limit + 1)
    finally:
        os.close(descriptor)


def _write_json_exclusive(path: Path, value: object) -> None:
    data = json.dumps(_jsonable(value), indent=2, sort_keys=True).encode() + b"\n"
    _write_exclusive(path, data)


def _write_exclusive(path: Path, data: bytes) -> None:
    directory = _open_directory(path.parent)
    try:
        descriptor = os.open(
            path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
    finally:
        os.close(directory)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
    finally:
        os.close(descriptor)


def _jsonable(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)
        }
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def _open_directory(path: Path) -> int:
    absolute = path.absolute()
    descriptor = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in absolute.parts[1:]:
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _file_digest_at(directory: int, name: str) -> tuple[str, int]:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError(f"file is not regular: {name}")
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(descriptor, 64 << 10):
            digest.update(chunk)
            size += len(chunk)
        return digest.hexdigest(), size
    finally:
        os.close(descriptor)


def _load_json(data: bytes) -> Any:
    if len(data) > MAX_JSON_BYTES:
        raise ValueError("JSON document is too large")

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        decoded = json.loads(data.decode("utf-8"), object_pairs_hook=pairs)
    except UnicodeDecodeError as error:
        raise ValueError("JSON document is not UTF-8") from error
    _validate_json_budget(decoded)
    return decoded


def _validate_json_budget(value: Any) -> None:
    items = 0

    def walk(current: Any, depth: int) -> None:
        nonlocal items
        if depth > MAX_JSON_DEPTH:
            raise ValueError("JSON document is too deeply nested")
        children: Iterable[Any]
        if isinstance(current, dict):
            items += len(current)
            children = current.values()
        elif isinstance(current, list):
            items += len(current)
            children = current
        else:
            return
        if items > MAX_JSON_ITEMS:
            raise ValueError("JSON document contains too many items")
        for child in children:
            walk(child, depth + 1)

    walk(value, 0)


def _keys(value: Any, expected: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("JSON object shape differs")
    return value


def _manifest_from_json(data: bytes) -> EvidenceManifest:
    raw = _keys(_load_json(data), {"schema", "entries"})
    entries = tuple(
        ManifestEntry(**_keys(item, {"path", "sha256", "bytes"})) for item in raw["entries"]
    )
    manifest = EvidenceManifest(raw["schema"], entries)
    if type(manifest.schema) is not str or any(
        type(entry.path) is not str
        or type(entry.sha256) is not str
        or type(entry.bytes) is not int
        for entry in entries
    ):
        raise ValueError("manifest scalar type differs")
    return manifest


def _report_from_json(data: bytes) -> ExperimentReport:
    raw = _keys(_load_json(data), {"schema", "environment", "trials"})
    environment = _environment(raw["environment"])
    return ExperimentReport(
        raw["schema"], environment, tuple(_trial(item) for item in raw["trials"])
    )


def _trial_from_json(data: bytes) -> TrialEvidence:
    return _trial(_load_json(data))


def _environment(value: Any) -> EnvironmentRecord:
    raw = _keys(
        value,
        {
            "captured_at",
            "temporalio_version",
            "temporal_cli",
            "temporal_cli_path",
            "temporal_cli_sha256",
            "python_version",
            "python_executable_sha256",
            "os",
            "architecture",
            "run_label",
            "source_pins",
        },
    )
    pins = tuple(
        SourcePin(**_keys(pin, {"path", "sha256", "bytes"})) for pin in raw.pop("source_pins")
    )
    environment = EnvironmentRecord(**raw, source_pins=pins)
    if any(
        type(item) is not str
        for item in (
            environment.captured_at,
            environment.temporalio_version,
            environment.temporal_cli,
            environment.temporal_cli_path,
            environment.temporal_cli_sha256,
            environment.python_version,
            environment.python_executable_sha256,
            environment.os,
            environment.architecture,
            environment.run_label,
        )
    ) or any(
        type(pin.path) is not str
        or type(pin.sha256) is not str
        or type(pin.bytes) is not int
        for pin in pins
    ):
        raise ValueError("environment scalar type differs")
    return environment


def _trial(value: Any) -> TrialEvidence:
    raw = _keys(
        value,
        {
            "record",
            "verdict",
            "worker_processes",
            "fault_arrival",
            "started_at",
            "fault_at",
            "replacement_at",
            "completed_at",
        },
    )
    record_raw = _keys(
        raw["record"],
        {
            "scenario",
            "trial",
            "workflow_id",
            "run_id",
            "expected_output",
            "final_attempt",
            "final_worker_id",
            "acknowledged_offset",
            "observations",
            "batches",
        },
    )
    observations = tuple(_observation(item) for item in record_raw.pop("observations"))
    batches = tuple(_batch(item) for item in record_raw.pop("batches"))
    scenario = Scenario(record_raw.pop("scenario"))
    record = TrialRecord(
        **record_raw,
        scenario=scenario,
        observations=observations,
        batches=batches,
    )
    _validate_types(record)
    verdict = TrialVerdict(
        **_keys(
            raw["verdict"],
            {"valid", "naive_output", "retry_aware_output", "naive_duplicate_control_failed"},
        )
    )
    processes = tuple(
        ProcessReceipt(**_keys(item, {"worker_id", "pid", "exit_code"}))
        for item in raw["worker_processes"]
    )
    arrival_raw = raw["fault_arrival"]
    arrival = (
        None
        if arrival_raw is None
        else BarrierArrival(**_keys(arrival_raw, {"point", "workflow_id", "attempt", "worker_id"}))
    )
    trial = TrialEvidence(
        record,
        verdict,
        processes,
        arrival,
        raw["started_at"],
        raw["fault_at"],
        raw["replacement_at"],
        raw["completed_at"],
    )
    _validate_trial_scalar_types(trial)
    return trial


def _observation(value: Any) -> StreamObservation:
    raw = _keys(value, {"offset", "event"})
    event = StreamEvent(
        **_keys(
            raw["event"],
            {"logical_output_id", "kind", "attempt", "worker_id", "chunk_index", "text"},
        )
    )
    return StreamObservation(raw["offset"], event)


def _batch(value: Any) -> PublishedBatch:
    raw = _keys(value, {"publisher_id", "sequence", "activity_attempt", "offsets"})
    return PublishedBatch(
        raw["publisher_id"], raw["sequence"], raw["activity_attempt"], tuple(raw["offsets"])
    )


def _prefix(record: TrialRecord) -> str:
    return f"{record.scenario.value}-trial-{record.trial}"


def _validate_trial_scalar_types(trial: TrialEvidence) -> None:
    if (
        type(trial.verdict.valid) is not bool
        or type(trial.verdict.naive_output) is not str
        or type(trial.verdict.retry_aware_output) is not str
        or type(trial.verdict.naive_duplicate_control_failed) is not bool
        or type(trial.started_at) is not str
        or (trial.fault_at is not None and type(trial.fault_at) is not str)
        or (trial.replacement_at is not None and type(trial.replacement_at) is not str)
        or type(trial.completed_at) is not str
    ):
        raise ValueError("trial evidence scalar type differs")
    if any(
        type(process.worker_id) is not str
        or type(process.pid) is not int
        or type(process.exit_code) is not int
        for process in trial.worker_processes
    ):
        raise ValueError("process receipt scalar type differs")
    if trial.fault_arrival is not None:
        trial.fault_arrival.validate()
