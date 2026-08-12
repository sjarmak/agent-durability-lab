from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from temporalio.client import Client, WorkflowHandle, WorkflowHistory
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.worker import Replayer

from .barrier import BarrierArrival, BarrierServer
from .contract import (
    Scenario,
    StreamEvent,
    StreamObservation,
    TrialRecord,
    TrialVerdict,
    WorkflowInput,
    WorkflowResult,
)
from .history import inspect_published_batches
from .oracle import audit_trial
from .worker_process import WorkerProcess, launch_worker, stop_worker
from .workflow import EVENTS_TOPIC, StreamRetryWorkflow


@dataclass(frozen=True)
class TrialCapture:
    record: TrialRecord
    verdict: TrialVerdict
    history: WorkflowHistory
    worker_processes: tuple[tuple[str, int, int], ...]
    fault_arrival: BarrierArrival | None
    started_at: str
    fault_at: str | None
    replacement_at: str | None
    completed_at: str


async def run_trial(
    *,
    client: Client,
    project_root: Path,
    barrier: BarrierServer,
    scenario: Scenario,
    trial: int,
    run_label: str,
) -> TrialCapture:
    workflow_id = f"{run_label}-{scenario.value}-trial-{trial}"
    task_queue = f"workflow-stream-{uuid.uuid4()}"
    first_id = f"{workflow_id}/worker-1"
    first = await launch_worker(
        project_root=project_root,
        address=client.service_client.config.target_host,
        task_queue=task_queue,
        worker_id=first_id,
        barrier=barrier,
    )
    first_process_identity = f"{first.worker_id}/pid-{first.pid}"
    expected_arrival = BarrierArrival(scenario.value, workflow_id, 1, first_process_identity)
    if scenario is not Scenario.UNFAULTED:
        barrier.expect(expected_arrival)
    workers: list[WorkerProcess] = [first]
    started_at = _now()
    handle = await client.start_workflow(
        StreamRetryWorkflow.run,
        WorkflowInput(scenario, trial, "ABC"),
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=timedelta(seconds=40),
    )
    observations_task = asyncio.create_task(_collect(client, handle, workflow_id))
    fault_arrival: BarrierArrival | None = None
    fault_at: str | None = None
    replacement_at: str | None = None
    try:
        if scenario is not Scenario.UNFAULTED:
            fault_arrival = await asyncio.wait_for(barrier.next_arrival(), timeout=15)
            if fault_arrival != expected_arrival:
                raise RuntimeError("fault barrier identity differs")
            fault_at = _now()
            await stop_worker(first, kill=True)
            replacement = await launch_worker(
                project_root=project_root,
                address=client.service_client.config.target_host,
                task_queue=task_queue,
                worker_id=f"{workflow_id}/worker-2",
                barrier=None,
            )
            workers.append(replacement)
            replacement_at = _now()
        result = await asyncio.wait_for(handle.result(), timeout=35)
        observations = await asyncio.wait_for(observations_task, timeout=5)
        history = await handle.fetch_history()
        completed_at = _now()
    finally:
        for worker in reversed(workers):
            await stop_worker(worker, kill=False)
        if not observations_task.done():
            observations_task.cancel()
    if not isinstance(result, WorkflowResult):
        raise TypeError("Workflow result type differs")
    record = TrialRecord(
        scenario=scenario,
        trial=trial,
        workflow_id=workflow_id,
        run_id=history.run_id,
        expected_output="ABC",
        final_attempt=result.final_attempt,
        final_worker_id=result.final_worker_id,
        acknowledged_offset=result.acknowledged_offset,
        observations=observations,
        batches=inspect_published_batches(history),
    )
    verdict = audit_trial(record)
    await Replayer(workflows=[StreamRetryWorkflow]).replay_workflow(history)
    return TrialCapture(
        record=record,
        verdict=verdict,
        history=history,
        worker_processes=tuple(
            (worker.worker_id, worker.pid, _exit_code(worker)) for worker in workers
        ),
        fault_arrival=fault_arrival,
        started_at=started_at,
        fault_at=fault_at,
        replacement_at=replacement_at,
        completed_at=completed_at,
    )


async def _collect(
    client: Client,
    handle: WorkflowHandle[StreamRetryWorkflow, WorkflowResult],
    workflow_id: str,
) -> tuple[StreamObservation, ...]:
    stream = WorkflowStreamClient.create(client, workflow_id)
    observations: list[StreamObservation] = []
    async for item in stream.subscribe(EVENTS_TOPIC, result_type=StreamEvent):
        observations.append(StreamObservation(item.offset, item.data))
        if item.data.kind == "complete":
            await handle.signal(StreamRetryWorkflow.acknowledge, item.offset)
            break
    return tuple(observations)


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _exit_code(worker: WorkerProcess) -> int:
    if worker.process.returncode is None:
        raise RuntimeError("Worker process exit was not observed")
    return worker.process.returncode
