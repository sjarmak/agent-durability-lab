from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import Client
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment

from temporal_native.activities import TOOL_EFFECT_COMMITTED
from temporal_native.barrier import BarrierServer
from temporal_native.contract import AgentEvent, TurnInput, TurnResult
from temporal_native.destination import ControlledDestination, ProtectionMode
from temporal_native.model import MODEL_RESPONSE_BUILT
from temporal_native.worker_process import launch_worker, stop_worker
from temporal_native.workflow import AGENT_EVENTS_TOPIC, TemporalNativeAgentWorkflow


@pytest.mark.parametrize(
    ("mode", "expected_applied"),
    [(ProtectionMode.UNSAFE, 2), (ProtectionMode.IDEMPOTENT, 1)],
)
async def test_worker_death_after_tool_effect_exposes_destination_boundary(
    tmp_path: Path,
    mode: ProtectionMode,
    expected_applied: int,
) -> None:
    project_root = Path(__file__).parents[1]
    database_path = tmp_path / "destination.db"
    workspace_path = tmp_path / "fixture"
    destination = ControlledDestination.create(
        database_path=database_path,
        workspace_path=workspace_path,
        mode=mode,
    )
    environment = await WorkflowEnvironment.start_local()
    async with environment as temporal, BarrierServer() as barrier:
        config = temporal.client.config()
        config["plugins"] = [
            OpenAIAgentsPlugin(register_activities=False, add_temporal_spans=False)
        ]
        client = Client(**config)
        task_queue = f"native-process-{uuid.uuid4()}"
        first = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="worker-1",
            barrier_address=barrier.address,
            barrier_points=(TOOL_EFFECT_COMMITTED,),
        )
        handle = await client.start_workflow(
            TemporalNativeAgentWorkflow.run,
            TurnInput(task="apply fixture", content="durable process fixture\n"),
            id=f"native-baseline/process-{mode.value}",
            task_queue=task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        observed = await asyncio.wait_for(barrier.next_arrival(TOOL_EFFECT_COMMITTED), timeout=10)
        assert observed.activity_attempt == 1
        assert observed.worker_process == f"worker-1/pid-{first.pid}"
        await stop_worker(first, kill=True)

        replacement = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="worker-2",
        )
        try:
            result = await asyncio.wait_for(handle.result(), timeout=20)
        finally:
            await stop_worker(replacement, kill=False)

    assert isinstance(result, TurnResult)
    snapshot = destination.snapshot()
    assert len(snapshot.attempts) == 2
    assert [attempt.applied for attempt in snapshot.attempts].count(True) == expected_applied
    assert {attempt.logical_effect_id for attempt in snapshot.attempts} == {
        result.logical_effect_id
    }
    assert {attempt.generation for attempt in snapshot.attempts} == {1}


async def test_worker_death_after_model_response_retries_only_incomplete_model_call(
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).parents[1]
    database_path = tmp_path / "destination.db"
    workspace_path = tmp_path / "fixture"
    destination = ControlledDestination.create(
        database_path=database_path,
        workspace_path=workspace_path,
        mode=ProtectionMode.IDEMPOTENT,
    )
    environment = await WorkflowEnvironment.start_local()
    async with environment as temporal, BarrierServer() as barrier:
        config = temporal.client.config()
        config["plugins"] = [
            OpenAIAgentsPlugin(register_activities=False, add_temporal_spans=False)
        ]
        client = Client(**config)
        task_queue = f"native-model-process-{uuid.uuid4()}"
        first = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="model-worker-1",
            barrier_address=barrier.address,
            barrier_points=(MODEL_RESPONSE_BUILT,),
        )
        handle = await client.start_workflow(
            TemporalNativeAgentWorkflow.run,
            TurnInput(task="apply fixture", content="durable model fixture\n"),
            id="native-baseline/model-completion-loss",
            task_queue=task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        observed = await asyncio.wait_for(barrier.next_arrival(MODEL_RESPONSE_BUILT), timeout=10)
        assert observed.activity_attempt == 1
        assert observed.worker_process == f"model-worker-1/pid-{first.pid}"
        await stop_worker(first, kill=True)

        replacement = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="model-worker-2",
        )
        try:
            result = await asyncio.wait_for(handle.result(), timeout=20)
            history = [event async for event in handle.fetch_history_events()]
        finally:
            await stop_worker(replacement, kill=False)

    assert isinstance(result, TurnResult)
    assert len(destination.snapshot().attempts) == 1
    attempts = model_activity_attempts(history)
    assert [attempt for _, attempt in attempts[:2]] == [2, 1]
    assert attempts[0][0] != attempts[1][0]


async def test_worker_death_after_result_is_built_replays_without_reissuing_effects(
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).parents[1]
    database_path = tmp_path / "destination.db"
    workspace_path = tmp_path / "fixture"
    destination = ControlledDestination.create(
        database_path=database_path,
        workspace_path=workspace_path,
        mode=ProtectionMode.IDEMPOTENT,
    )
    environment = await WorkflowEnvironment.start_local()
    async with environment as temporal:
        config = temporal.client.config()
        config["plugins"] = [
            OpenAIAgentsPlugin(register_activities=False, add_temporal_spans=False)
        ]
        client = Client(**config)
        task_queue = f"native-result-process-{uuid.uuid4()}"
        workflow_id = "native-baseline/result-built-loss"
        first = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="result-worker-1",
        )
        handle = await client.start_workflow(
            TemporalNativeAgentWorkflow.run,
            TurnInput(
                task="apply fixture",
                content="durable result fixture\n",
                hold_result=True,
            ),
            id=workflow_id,
            task_queue=task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        stream = WorkflowStreamClient.create(
            client, workflow_id, batch_interval=timedelta(milliseconds=10)
        )
        result_event = await asyncio.wait_for(_next_agent_event(stream, "result_built"), timeout=10)
        assert result_event.session_id == workflow_id
        await stop_worker(first, kill=True)

        replacement = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="result-worker-2",
        )
        try:
            status = await handle.query(TemporalNativeAgentWorkflow.status)
            assert status.phase == "result_built"
            await handle.signal(TemporalNativeAgentWorkflow.release_result)
            result = await asyncio.wait_for(handle.result(), timeout=10)
            history = [event async for event in handle.fetch_history_events()]
        finally:
            await stop_worker(replacement, kill=False)

    assert result.destination_receipt == result_event.detail
    assert len(destination.snapshot().attempts) == 1
    scheduled = scheduled_activity_types(history)
    assert scheduled.count("invoke_model_activity_streaming") == 2
    assert scheduled.count("apply_fixture_change") == 1
    assert scheduled.count("record_agent_cleanup") == 1


def model_activity_attempts(history: list[HistoryEvent]) -> list[tuple[int, int]]:
    scheduled_types: dict[int, str] = {}
    attempts: list[tuple[int, int]] = []
    for event in history:
        if event.HasField("activity_task_scheduled_event_attributes"):
            attributes = event.activity_task_scheduled_event_attributes
            scheduled_types[event.event_id] = attributes.activity_type.name
        elif event.HasField("activity_task_started_event_attributes"):
            attributes = event.activity_task_started_event_attributes
            activity_type = scheduled_types.get(attributes.scheduled_event_id)
            if activity_type == "invoke_model_activity_streaming":
                attempts.append((attributes.scheduled_event_id, attributes.attempt))
    return attempts


async def _next_agent_event(stream: WorkflowStreamClient, expected_kind: str) -> AgentEvent:
    async for item in stream.subscribe(AGENT_EVENTS_TOPIC, result_type=AgentEvent):
        if item.data.kind == expected_kind:
            return item.data
    raise AssertionError(f"stream ended before {expected_kind}")


def scheduled_activity_types(history: list[HistoryEvent]) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history
        if event.HasField("activity_task_scheduled_event_attributes")
    ]
