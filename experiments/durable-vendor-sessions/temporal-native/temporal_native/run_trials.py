"""Run and preserve live ambiguous-effect trials through the common oracle."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from temporalio import __version__ as temporalio_version
from temporalio.client import Client, WorkflowHandle, WorkflowHistory
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.testing import WorkflowEnvironment

from temporal_native.activities import TOOL_EFFECT_COMMITTED
from temporal_native.barrier import BarrierArrival, BarrierServer
from temporal_native.contract import TurnInput, TurnResult
from temporal_native.destination import (
    ControlledDestination,
    DestinationSnapshot,
    ProtectionMode,
)
from temporal_native.worker_process import WorkerProcess, launch_worker, stop_worker
from temporal_native.workflow import TemporalNativeAgentWorkflow


@dataclass(frozen=True)
class FaultedTrial:
    handle: WorkflowHandle[TemporalNativeAgentWorkflow, TurnResult]
    first_worker: WorkerProcess
    arrival: BarrierArrival
    started_at: datetime
    triggered_at: datetime


@dataclass(frozen=True)
class TrialObservation:
    faulted: FaultedTrial
    recovery_worker: WorkerProcess
    recovery_started_at: datetime
    completed_at: datetime
    result: TurnResult
    history: WorkflowHistory
    snapshot: DestinationSnapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=3)
    return parser


async def run(args: argparse.Namespace) -> None:
    if args.trials < 1:
        raise ValueError("--trials must be positive")
    experiment_root = Path(__file__).parents[1]
    repository_root = Path(__file__).parents[4]
    adapter_root = repository_root / "experiments/durable-vendor-sessions/temporal-native"
    adapter_version = _source_hash(
        repository_root,
        (
            adapter_root / "evidenceadapter/adapter.go",
            adapter_root / "cmd/temporal-native-evidence/main.go",
        ),
    )
    evidence_root = args.evidence_root.resolve()
    evidence_root.mkdir(mode=0o750, parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="temporal-native-adapter-") as binary_dir:
        adapter_binary = Path(binary_dir) / "temporal-native-evidence"
        await _run_command(
            "go",
            "build",
            "-o",
            str(adapter_binary),
            "./experiments/durable-vendor-sessions/temporal-native/cmd/temporal-native-evidence",
            cwd=repository_root,
        )
        for trial in range(1, args.trials + 1):
            for mode in (ProtectionMode.UNSAFE, ProtectionMode.IDEMPOTENT):
                probe = "unsafe" if mode is ProtectionMode.UNSAFE else "protected"
                capture = await _run_trial(
                    experiment_root=experiment_root,
                    mode=mode,
                    probe=probe,
                    trial=trial,
                    adapter_version=adapter_version,
                )
                output = await _write_capture(
                    capture, adapter_binary, evidence_root, repository_root, probe, trial
                )
                print(output.strip(), flush=True)


async def _write_capture(
    capture: dict[str, object],
    adapter_binary: Path,
    evidence_root: Path,
    repository_root: Path,
    probe: str,
    trial: int,
) -> str:
    with tempfile.TemporaryDirectory(prefix=f"temporal-native-{probe}-{trial}-") as capture_dir:
        capture_path = Path(capture_dir) / "capture.json"
        capture_path.write_text(
            json.dumps(capture, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return await _run_command(
            str(adapter_binary),
            "--capture",
            str(capture_path),
            "--evidence-root",
            str(evidence_root),
            cwd=repository_root,
        )


async def _run_trial(
    *,
    experiment_root: Path,
    mode: ProtectionMode,
    probe: str,
    trial: int,
    adapter_version: str,
) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix=f"temporal-native-live-{probe}-{trial}-") as trial_dir:
        trial_root = Path(trial_dir)
        database_path = trial_root / "destination.db"
        workspace_path = trial_root / "fixture"
        destination = ControlledDestination.create(
            database_path=database_path,
            workspace_path=workspace_path,
            mode=mode,
        )
        environment = await WorkflowEnvironment.start_local()
        async with environment as temporal, BarrierServer() as barrier:
            client = _trial_client(temporal.client)
            task_queue = f"native-evidence-{uuid.uuid4()}"
            session_id = f"temporal-native/ambiguous-effect/{probe}/trial/{trial}"
            faulted = await _fault_first_worker(
                experiment_root,
                client,
                barrier,
                destination,
                task_queue,
                database_path,
                workspace_path,
                session_id,
                probe,
                trial,
            )
            observation = await _recover_trial(
                experiment_root,
                client,
                destination,
                faulted,
                task_queue,
                database_path,
                workspace_path,
                probe,
                trial,
            )
        _validate_observation(observation, mode)
        return _build_capture(observation, experiment_root, mode, probe, trial, adapter_version)


def _trial_client(client: Client) -> Client:
    config = client.config()
    config["plugins"] = [OpenAIAgentsPlugin(register_activities=False, add_temporal_spans=False)]
    return Client(**config)


async def _fault_first_worker(
    experiment_root: Path,
    client: Client,
    barrier: BarrierServer,
    destination: ControlledDestination,
    task_queue: str,
    database_path: Path,
    workspace_path: Path,
    session_id: str,
    probe: str,
    trial: int,
) -> FaultedTrial:
    first = await launch_worker(
        project_root=experiment_root,
        address=client.service_client.config.target_host,
        task_queue=task_queue,
        database_path=database_path,
        workspace_path=workspace_path,
        worker_id=f"{probe}-trial-{trial}-worker-1",
        barrier_address=barrier.address,
        barrier_points=(TOOL_EFFECT_COMMITTED,),
    )
    started_at = datetime.now(UTC)
    try:
        handle = await client.start_workflow(
            TemporalNativeAgentWorkflow.run,
            TurnInput(task="apply fixture", content="durable evidence fixture\n"),
            id=session_id,
            task_queue=task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        arrival = await asyncio.wait_for(barrier.next_arrival(TOOL_EFFECT_COMMITTED), timeout=10)
        if len(destination.snapshot().attempts) != 1 or arrival.activity_attempt != 1:
            raise RuntimeError("fault barrier did not isolate the first tool attempt")
        triggered_at = datetime.now(UTC)
        await stop_worker(first, kill=True)
        return FaultedTrial(handle, first, arrival, started_at, triggered_at)
    except BaseException:
        await stop_worker(first, kill=True)
        raise


async def _recover_trial(
    experiment_root: Path,
    client: Client,
    destination: ControlledDestination,
    faulted: FaultedTrial,
    task_queue: str,
    database_path: Path,
    workspace_path: Path,
    probe: str,
    trial: int,
) -> TrialObservation:
    recovery = await launch_worker(
        project_root=experiment_root,
        address=client.service_client.config.target_host,
        task_queue=task_queue,
        database_path=database_path,
        workspace_path=workspace_path,
        worker_id=f"{probe}-trial-{trial}-worker-2",
    )
    recovery_started_at = datetime.now(UTC)
    try:
        result = await asyncio.wait_for(faulted.handle.result(), timeout=20)
        completed_at = datetime.now(UTC)
        history = await faulted.handle.fetch_history()
    finally:
        await stop_worker(recovery, kill=False)
    return TrialObservation(
        faulted,
        recovery,
        recovery_started_at,
        completed_at,
        result,
        history,
        destination.snapshot(),
    )


def _validate_observation(observation: TrialObservation, mode: ProtectionMode) -> None:
    expected_applied = [True, mode is ProtectionMode.UNSAFE]
    observed_applied = [item.applied for item in observation.snapshot.attempts]
    if len(observation.snapshot.attempts) != 2 or observed_applied != expected_applied:
        raise RuntimeError("destination observations do not match the selected probe")
    if observation.result.logical_effect_id != observation.snapshot.attempts[0].logical_effect_id:
        raise RuntimeError("Workflow result and destination logical effects differ")


def _build_capture(
    observed: TrialObservation,
    experiment_root: Path,
    mode: ProtectionMode,
    probe: str,
    trial: int,
    adapter_version: str,
) -> dict[str, object]:
    return {
        "adapter_version": adapter_version,
        "trial": trial,
        "probe": probe,
        "session_id": observed.result.session_id,
        "destination_id": observed.snapshot.destination_id,
        "logical_effect_id": observed.result.logical_effect_id,
        "generation": observed.result.generation,
        "agent_source_sha256": _source_hash(
            experiment_root,
            tuple(sorted((experiment_root / "temporal_native").glob("*.py"))),
        ),
        "runtime": f"Python {platform.python_version()}; temporalio {temporalio_version}",
        "started_at": _utc(observed.faulted.started_at),
        "first_worker": {"actor_id": "agent-1", "identity": observed.faulted.first_worker.identity},
        "recovery_worker": {"actor_id": "agent-1", "identity": observed.recovery_worker.identity},
        "attempts": [
            {
                "physical_attempt_id": item.physical_attempt_id,
                "applied": item.applied,
                "observed_at": _utc(item.observed_at),
            }
            for item in observed.snapshot.attempts
        ],
        "fault": {
            "point": TOOL_EFFECT_COMMITTED,
            "triggered_at": _utc(observed.faulted.triggered_at),
        },
        "completed_at": _utc(observed.completed_at),
        "history": _native_records(observed),
        "settings": {
            "destination_mode": mode.value,
            "openai_agents": "0.19.4",
            "temporal_server": "1.31.2",
        },
    }


def _native_records(observed: TrialObservation) -> list[dict[str, str]]:
    history_json = json.loads(observed.history.to_json())
    records = [
        {
            "kind": "temporal_history_event",
            "detail": json.dumps(event, sort_keys=True, separators=(",", ":")),
        }
        for event in history_json["events"]
    ]
    controls = (
        {
            "action": "worker_ready",
            "identity": observed.faulted.first_worker.identity,
            "at": _utc(observed.faulted.started_at),
        },
        {
            "action": "sigkill",
            "identity": observed.faulted.first_worker.identity,
            "at": _utc(observed.faulted.triggered_at),
        },
        {
            "action": "worker_ready",
            "identity": observed.recovery_worker.identity,
            "at": _utc(observed.recovery_started_at),
        },
    )
    return [
        *records,
        {"kind": "barrier_arrival", "detail": observed.faulted.arrival.model_dump_json()},
        {
            "kind": "process_controls",
            "detail": json.dumps(controls, sort_keys=True, separators=(",", ":")),
        },
        {"kind": "destination_snapshot", "detail": observed.snapshot.model_dump_json()},
    ]


async def _run_command(*command: str, cwd: Path) -> str:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"command failed ({process.returncode}): {' '.join(command)}\n{stderr.decode()}"
        )
    return stdout.decode()


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _source_hash(root: Path, paths: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\x00")
        digest.update(path.read_bytes())
        digest.update(b"\x00")
    return digest.hexdigest()


def main(argv: Sequence[str] | None = None) -> None:
    asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":
    main()
