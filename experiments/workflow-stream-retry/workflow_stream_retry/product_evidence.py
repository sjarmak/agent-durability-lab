from __future__ import annotations

import dataclasses
import hashlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import types
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, get_args, get_origin, get_type_hints

from temporalio import __version__ as temporalio_version
from temporalio.api.enums.v1 import TimeoutType
from temporalio.client import WorkflowHistory
from temporalio.converter import DataConverter
from temporalio.worker import Replayer

from .barrier import BarrierArrival
from .evidence import (
    MAX_JSON_BYTES,
    ManifestEntry,
    SourcePin,
    _file_digest,
    _keys,
    _load_json,
    _open_directory,
    _read_regular,
    _read_regular_at,
    _write_exclusive,
    _write_json_exclusive,
)
from .history import activity_attempts, activity_retry_failures
from .product_contract import (
    Arm,
    ProductActivityResult,
    ProductScenario,
    ProductTrialRecord,
    ProductTrialVerdict,
    ProductWorkflowInput,
    ProductWorkflowResult,
)
from .product_history import inspect_product_batch_actors, inspect_product_stream
from .product_metrics import RecoverySurfaceMetrics, measure_recovery_surface
from .product_population import ProductPopulationSummary, audit_product_population
from .product_runner import ProductTrialCapture
from .product_workflow import ProductStreamWorkflow

SCHEMA = "workflow-stream-product-evidence-v1"
SDK_PYTHON_COMMIT = "d489a5dd679094f6580556dc531c9f1e1515b804"
SDK_CORE_COMMIT = "999e5a7dc8bbb8c457322ccb8e1806a0e780be95"
PATCH_SHA256 = "2d72c81965753074cd1d8305c2ab9767e1bd1a1811ea7ee2f43fdd1d3bcb0684"
TEMPORAL_CLI_VERSION = "temporal version 1.8.0 (Server 1.31.2, UI 2.50.1)"
PYTHON_VERSION = "3.12.3"


@dataclass(frozen=True)
class ProductEnvironmentRecord:
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
    sdk_python_root: str
    sdk_python_commit: str
    sdk_core_commit: str
    patch_sha256: str
    source_pins: tuple[SourcePin, ...]


@dataclass(frozen=True)
class ProductProcessReceipt:
    worker_id: str
    pid: int
    exit_code: int

    @property
    def identity(self) -> str:
        return f"{self.worker_id}/pid-{self.pid}"


@dataclass(frozen=True)
class ProductTrialEvidence:
    record: ProductTrialRecord
    verdict: ProductTrialVerdict
    worker_processes: tuple[ProductProcessReceipt, ...]
    fault_arrival: BarrierArrival | None
    started_at: str
    fault_at: str | None
    replacement_at: str | None
    completed_at: str


@dataclass(frozen=True)
class ProductExperimentReport:
    schema: str
    environment: ProductEnvironmentRecord
    recovery_surface: RecoverySurfaceMetrics
    summary: ProductPopulationSummary
    trials: tuple[ProductTrialEvidence, ...]


@dataclass(frozen=True)
class ProductEvidenceManifest:
    schema: str
    entries: tuple[ManifestEntry, ...]


def product_trial_evidence(capture: ProductTrialCapture) -> ProductTrialEvidence:
    return ProductTrialEvidence(
        record=capture.record,
        verdict=capture.verdict,
        worker_processes=tuple(
            ProductProcessReceipt(*process) for process in capture.worker_processes
        ),
        fault_arrival=capture.fault_arrival,
        started_at=capture.started_at,
        fault_at=capture.fault_at,
        replacement_at=capture.replacement_at,
        completed_at=capture.completed_at,
    )


def build_product_environment(
    repo_root: Path,
    project_root: Path,
    sdk_python_root: Path,
    run_label: str,
) -> ProductEnvironmentRecord:
    _require_python_version(platform.python_version())
    temporal = shutil.which("temporal")
    if temporal is None:
        raise RuntimeError("Temporal CLI is required")
    temporal_path = Path(temporal).resolve(strict=True)
    temporal_cli = subprocess.run(
        [str(temporal_path), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if temporal_cli != TEMPORAL_CLI_VERSION:
        raise ValueError("Temporal CLI version differs from the registered pin")
    sdk_root = sdk_python_root.resolve(strict=True)
    sdk_commit = _git_revision(sdk_root)
    core_commit = _git_revision(sdk_root / "temporalio" / "bridge" / "sdk-core")
    if sdk_commit != SDK_PYTHON_COMMIT or core_commit != SDK_CORE_COMMIT:
        raise ValueError("SDK source revision differs from the registered pin")
    patch_path = (
        repo_root
        / "contrib"
        / "sdk-python-retry-aware-streams"
        / "sdk-python-d489a5d-retry-aware-streams.patch"
    )
    _require_candidate_patch(sdk_root, patch_path)
    pins = _source_pins(repo_root, project_root)
    patch = next(
        pin for pin in pins if pin.path.endswith("sdk-python-d489a5d-retry-aware-streams.patch")
    )
    if patch.sha256 != PATCH_SHA256:
        raise ValueError("SDK candidate patch digest differs from the registered pin")
    return ProductEnvironmentRecord(
        captured_at=_now(),
        temporalio_version=temporalio_version,
        temporal_cli=temporal_cli,
        temporal_cli_path=str(temporal_path),
        temporal_cli_sha256=_file_digest(temporal_path)[0],
        python_version=platform.python_version(),
        python_executable_sha256=_file_digest(Path(sys.executable).resolve(strict=True))[0],
        os=platform.system().lower(),
        architecture=platform.machine(),
        run_label=run_label,
        sdk_python_root=str(sdk_root),
        sdk_python_commit=sdk_commit,
        sdk_core_commit=core_commit,
        patch_sha256=patch.sha256,
        source_pins=pins,
    )


def preserve_product_trial(root: Path, capture: ProductTrialCapture) -> ProductTrialEvidence:
    evidence = product_trial_evidence(capture)
    prefix = _prefix(evidence.record)
    _write_exclusive(
        root / f"{prefix}-history.json",
        capture.history.to_json().encode("utf-8") + b"\n",
    )
    _write_json_exclusive(root / f"{prefix}-trial.json", evidence)
    return evidence


def preserve_product_report(root: Path, report: ProductExperimentReport) -> None:
    _write_json_exclusive(root / "report.json", report)
    entries = _inventory(root, exclude_manifest=True)
    _write_json_exclusive(root / "manifest.json", ProductEvidenceManifest(SCHEMA, entries))


def preserve_product_failure(root: Path, error: BaseException) -> None:
    with suppress(FileExistsError):
        _write_json_exclusive(root / "failure.json", {"schema": SCHEMA, "error": str(error)})


async def audit_product_evidence(
    root: Path,
    repo_root: Path,
    project_root: Path,
) -> ProductExperimentReport:
    directory = _open_directory(root)
    try:
        manifest = _decode(
            ProductEvidenceManifest,
            _load_json(_read_regular_at(directory, "manifest.json")),
        )
        if manifest.schema != SCHEMA:
            raise ValueError("product evidence manifest schema differs")
        _validate_inventory(manifest.entries)
        bundle = _read_bundle(directory, manifest.entries)
        report = _decode(ProductExperimentReport, _load_json(bundle["report.json"]))
        if report.schema != SCHEMA:
            raise ValueError("product evidence report schema differs")
        _validate_environment(report.environment, root, repo_root, project_root)
        views: list[tuple[ProductTrialRecord, ProductTrialVerdict]] = []
        for stored in report.trials:
            expected_workflow_id = (
                f"{report.environment.run_label}-{stored.record.arm.value}-"
                f"{stored.record.scenario.value}-trial-{stored.record.trial}"
            )
            if stored.record.workflow_id != expected_workflow_id:
                raise ValueError("product Workflow identity differs from the schedule")
            prefix = _prefix(stored.record)
            trial = _decode(
                ProductTrialEvidence,
                _load_json(bundle[f"{prefix}-trial.json"]),
            )
            if trial != stored:
                raise ValueError("stored product trial views differ")
            history_bytes = bundle[f"{prefix}-history.json"]
            history = WorkflowHistory.from_json(
                trial.record.workflow_id, history_bytes.decode("utf-8")
            )
            _audit_trial(
                trial,
                history,
                len(history_bytes.rstrip(b"\n")),
                report.environment.captured_at,
            )
            await Replayer(workflows=[ProductStreamWorkflow]).replay_workflow(history)
            views.append((trial.record, trial.verdict))
        summary = audit_product_population(tuple(views))
        if summary != report.summary:
            raise ValueError("product population summary differs")
        if report.recovery_surface != measure_recovery_surface(project_root):
            raise ValueError("recovery surface measurement differs")
        return report
    finally:
        os.close(directory)


def _audit_trial(
    trial: ProductTrialEvidence,
    history: WorkflowHistory,
    history_json_bytes: int,
    captured_at: str,
) -> None:
    record = trial.record
    if history.run_id != record.run_id:
        raise ValueError("Workflow history run identity differs")
    observations, batch_count = inspect_product_stream(history, record.arm)
    if (
        observations != record.observations
        or batch_count != record.stream_batch_count
        or len(history.events) != record.history_event_count
        or history_json_bytes != record.history_json_bytes
    ):
        raise ValueError("Workflow history metrics or stream evidence differ")
    if _workflow_input(history) != ProductWorkflowInput(
        record.arm, record.scenario, record.trial, record.expected_output
    ):
        raise ValueError("Workflow history input differs")
    expected_result = ProductWorkflowResult(
        record.expected_output,
        record.final_attempt,
        record.final_worker_id,
        record.final_terminal,
        record.acknowledgement,
    )
    if _workflow_result(history) != expected_result:
        raise ValueError("Workflow history result differs")
    activity_result = _activity_result(history)
    if (
        activity_result.full_text != record.expected_output
        or activity_result.attempt != record.final_attempt
        or activity_result.worker_id != record.final_worker_id
        or activity_result.terminal != record.final_terminal
    ):
        raise ValueError("Activity history result differs")
    if _logical_ack_rejections(history) != record.stale_ack_rejections:
        raise ValueError("logical output acknowledgement rejection count differs")
    _validate_processes(trial, history, captured_at)


def _validate_processes(
    trial: ProductTrialEvidence, history: WorkflowHistory, captured_at: str
) -> None:
    record = trial.record
    expected_count = 1 if record.scenario is ProductScenario.UNFAULTED else 2
    if len(trial.worker_processes) != expected_count:
        raise ValueError("Worker process count differs")
    expected_workers = tuple(
        f"{record.workflow_id}/worker-{index}" for index in range(1, expected_count + 1)
    )
    if (
        tuple(process.worker_id for process in trial.worker_processes) != expected_workers
        or any(process.pid <= 0 for process in trial.worker_processes)
        or record.final_worker_id != trial.worker_processes[-1].identity
    ):
        raise ValueError("Worker process identity differs")
    expected_attempts = (
        ((1, trial.worker_processes[0].identity),)
        if expected_count == 1
        else ((2, trial.worker_processes[1].identity),)
    )
    if activity_attempts(history) != expected_attempts:
        raise ValueError("Activity attempt process identity differs")
    expected_failures = (
        ((1, None),) if expected_count == 1 else ((2, int(TimeoutType.TIMEOUT_TYPE_HEARTBEAT)),)
    )
    if activity_retry_failures(history) != expected_failures:
        raise ValueError("Activity retry failure differs")
    workers_by_attempt = {
        1: trial.worker_processes[0],
        expected_count: trial.worker_processes[-1],
    }
    if any(
        observation.event.activity_attempt not in workers_by_attempt
        or observation.event.worker_id
        != workers_by_attempt[observation.event.activity_attempt].identity
        for observation in record.observations
    ):
        raise ValueError("stream event Worker process identity differs")
    actors = inspect_product_batch_actors(history, record.arm)
    if any(
        actor.activity_attempt not in workers_by_attempt
        or not actor.identity.startswith(f"{workers_by_attempt[actor.activity_attempt].pid}@")
        or not actor.identity.split("@", 1)[1]
        for actor in actors
    ):
        raise ValueError("stream Signal actor differs from the Worker process")
    if trial.worker_processes[-1].exit_code != -15:
        raise ValueError("final Worker did not exit by controlled termination")
    timestamps = [_utc(trial.started_at)]
    if expected_count == 1:
        if any(
            value is not None
            for value in (trial.fault_arrival, trial.fault_at, trial.replacement_at)
        ):
            raise ValueError("unfaulted trial contains a fault receipt")
    else:
        expected_arrival = BarrierArrival(
            record.scenario.value,
            record.workflow_id,
            1,
            trial.worker_processes[0].identity,
        )
        if (
            trial.fault_arrival != expected_arrival
            or trial.worker_processes[0].exit_code != -9
            or trial.fault_at is None
            or trial.replacement_at is None
        ):
            raise ValueError("faulted Worker receipt differs")
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


def _validate_environment(
    environment: ProductEnvironmentRecord,
    root: Path,
    repo_root: Path,
    project_root: Path,
) -> None:
    temporal = shutil.which("temporal")
    if (
        environment.run_label != root.name
        or environment.temporalio_version != temporalio_version
        or environment.temporal_cli != TEMPORAL_CLI_VERSION
        or temporal is None
        or Path(environment.temporal_cli_path) != Path(temporal).resolve(strict=True)
        or environment.temporal_cli_sha256 != _file_digest(Path(environment.temporal_cli_path))[0]
        or environment.python_version != PYTHON_VERSION
        or environment.python_version != platform.python_version()
        or environment.python_executable_sha256
        != _file_digest(Path(sys.executable).resolve(strict=True))[0]
        or environment.os != platform.system().lower()
        or environment.architecture != platform.machine()
        or environment.sdk_python_commit != SDK_PYTHON_COMMIT
        or environment.sdk_core_commit != SDK_CORE_COMMIT
        or environment.patch_sha256 != PATCH_SHA256
        or not Path(environment.sdk_python_root).is_absolute()
        or environment.source_pins != _source_pins(repo_root, project_root)
    ):
        raise ValueError("product environment provenance differs")
    _utc(environment.captured_at)


def _workflow_input(history: WorkflowHistory) -> ProductWorkflowInput:
    converter = DataConverter.default.payload_converter
    values = []
    for event in history.events:
        if event.HasField("workflow_execution_started_event_attributes"):
            values.extend(
                converter.from_payloads(
                    event.workflow_execution_started_event_attributes.input.payloads,
                    [ProductWorkflowInput],
                )
            )
    if len(values) != 1 or not isinstance(values[0], ProductWorkflowInput):
        raise ValueError("Workflow input count or type differs")
    return values[0]


def _workflow_result(history: WorkflowHistory) -> ProductWorkflowResult:
    converter = DataConverter.default.payload_converter
    values = []
    for event in history.events:
        if event.HasField("workflow_execution_completed_event_attributes"):
            values.extend(
                converter.from_payloads(
                    event.workflow_execution_completed_event_attributes.result.payloads,
                    [ProductWorkflowResult],
                )
            )
    if len(values) != 1 or not isinstance(values[0], ProductWorkflowResult):
        raise ValueError("Workflow result count or type differs")
    return values[0]


def _activity_result(history: WorkflowHistory) -> ProductActivityResult:
    converter = DataConverter.default.payload_converter
    values = []
    for event in history.events:
        if event.HasField("activity_task_completed_event_attributes"):
            values.extend(
                converter.from_payloads(
                    event.activity_task_completed_event_attributes.result.payloads,
                    [ProductActivityResult],
                )
            )
    if len(values) != 1 or not isinstance(values[0], ProductActivityResult):
        raise ValueError("Activity result count or type differs")
    return values[0]


def _logical_ack_rejections(history: WorkflowHistory) -> int:
    rejected = 0
    for event in history.events:
        if not event.HasField("workflow_execution_update_completed_event_attributes"):
            continue
        outcome = event.workflow_execution_update_completed_event_attributes.outcome
        if (
            outcome.HasField("failure")
            and outcome.failure.HasField("application_failure_info")
            and outcome.failure.application_failure_info.type
            == "LogicalOutputAcknowledgementRejected"
        ):
            rejected += 1
    return rejected


def _source_pins(repo_root: Path, project_root: Path) -> tuple[SourcePin, ...]:
    paths = [project_root / "pyproject.toml", project_root / "uv.lock"]
    paths.append(project_root / "sitecustomize.py")
    paths.extend(sorted((project_root / "workflow_stream_retry").glob("product_*.py")))
    paths.extend(
        [
            project_root / "workflow_stream_retry" / "run_product_experiment.py",
            project_root / "workflow_stream_retry" / "barrier.py",
            project_root / "workflow_stream_retry" / "worker_process.py",
            repo_root
            / "contrib"
            / "sdk-python-retry-aware-streams"
            / "sdk-python-d489a5d-retry-aware-streams.patch",
        ]
    )
    pins = []
    for path in paths:
        digest, size = _file_digest(path)
        pins.append(SourcePin(path.relative_to(repo_root).as_posix(), digest, size))
    return tuple(pins)


def _git_revision(path: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _require_candidate_patch(sdk_root: Path, patch_path: Path) -> None:
    candidate = subprocess.run(
        ["git", "diff", "--full-index", "--binary", "--no-ext-diff"],
        cwd=sdk_root,
        check=True,
        capture_output=True,
    ).stdout
    patch = _read_regular(patch_path, limit=None)
    if candidate != patch:
        raise ValueError("SDK working-tree diff differs from the candidate patch")
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=sdk_root,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    if any(
        path and not Path(path.decode("utf-8")).name.startswith(".coverage") for path in untracked
    ):
        raise ValueError("SDK working tree contains an unpinned untracked file")


def _require_python_version(version: str) -> None:
    if version != PYTHON_VERSION:
        raise ValueError(f"Python version differs from registered {PYTHON_VERSION}: {version}")


def _prefix(record: ProductTrialRecord) -> str:
    return f"{record.arm.value}-{record.scenario.value}-trial-{record.trial}"


def _expected_inventory() -> set[str]:
    names = {"report.json"}
    for arm in Arm:
        for scenario in ProductScenario:
            for trial in range(1, 4):
                prefix = f"{arm.value}-{scenario.value}-trial-{trial}"
                names.update({f"{prefix}-history.json", f"{prefix}-trial.json"})
    return names


def _inventory(root: Path, *, exclude_manifest: bool) -> tuple[ManifestEntry, ...]:
    directory = _open_directory(root)
    try:
        names = set(os.listdir(directory))
        if exclude_manifest:
            names.discard("manifest.json")
        if names != _expected_inventory():
            raise ValueError("product evidence inventory is not exact")
        entries = []
        for name in sorted(names):
            info = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_JSON_BYTES:
                raise ValueError("product evidence entry is invalid")
            data = _read_regular_at(directory, name)
            entries.append(ManifestEntry(name, hashlib.sha256(data).hexdigest(), len(data)))
        return tuple(entries)
    finally:
        os.close(directory)


def _validate_inventory(entries: tuple[ManifestEntry, ...]) -> None:
    if {entry.path for entry in entries} != _expected_inventory() or len(entries) != len(
        _expected_inventory()
    ):
        raise ValueError("product evidence manifest inventory differs")


def _read_bundle(directory: int, entries: tuple[ManifestEntry, ...]) -> dict[str, bytes]:
    names = set(os.listdir(directory))
    names.discard("manifest.json")
    indexed = {entry.path: entry for entry in entries}
    if names != _expected_inventory() or set(indexed) != names:
        raise ValueError("product evidence bundle inventory differs")
    bundle = {}
    for name in sorted(names):
        data = _read_regular_at(directory, name)
        entry = indexed[name]
        if len(data) != entry.bytes or hashlib.sha256(data).hexdigest() != entry.sha256:
            raise ValueError("product evidence entry digest differs")
        bundle[name] = data
    return bundle


T = TypeVar("T")


def _decode(expected: type[T], value: Any) -> T:  # noqa: UP047
    decoded = _decode_value(expected, value)
    if not isinstance(decoded, expected):
        raise ValueError("decoded evidence type differs")
    return decoded


def _decode_value(expected: Any, value: Any) -> Any:
    origin = get_origin(expected)
    arguments = get_args(expected)
    if origin is types.UnionType:
        if value is None and type(None) in arguments:
            return None
        choices = [argument for argument in arguments if argument is not type(None)]
        if len(choices) != 1:
            raise ValueError("unsupported evidence union")
        return _decode_value(choices[0], value)
    if origin is tuple:
        if not isinstance(value, list) or len(arguments) != 2 or arguments[1] is not Ellipsis:
            raise ValueError("evidence tuple shape differs")
        return tuple(_decode_value(arguments[0], item) for item in value)
    if isinstance(expected, type) and issubclass(expected, Enum):
        if type(value) is not str:
            raise ValueError("evidence enum scalar differs")
        return expected(value)
    if isinstance(expected, type) and dataclasses.is_dataclass(expected):
        raw = _keys(value, {field.name for field in dataclasses.fields(expected)})
        hints = get_type_hints(expected)
        return expected(
            **{
                field.name: _decode_value(hints[field.name], raw[field.name])
                for field in dataclasses.fields(expected)
            }
        )
    if expected in (str, int, bool, float):
        if type(value) is not expected:
            raise ValueError("evidence scalar type differs")
        return value
    if expected is Any:
        return value
    raise ValueError(f"unsupported evidence type {expected!r}")


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp is not UTC")
    return parsed


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
