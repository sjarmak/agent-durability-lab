from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.client import WorkflowContinuedAsNewError
from temporalio.common import RawValue
from temporalio.contrib.openai_agents import ModelActivityParameters
from temporalio.contrib.openai_agents.testing import AgentEnvironment
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_native.activities import ToolActivities, ToolRuntime
from temporal_native.contract import AgentEvent, TurnInput
from temporal_native.destination import ControlledDestination, ProtectionMode
from temporal_native.model import FixtureModel, ModelRuntime
from temporal_native.workflow import (
    AGENT_EVENTS_TOPIC,
    STREAM_DONE_TOPIC,
    TemporalNativeAgentWorkflow,
)


async def test_stream_follows_continue_as_new_and_preserves_session_owner(
    tmp_path: Path,
) -> None:
    destination = ControlledDestination.create(
        database_path=tmp_path / "destination.db",
        workspace_path=tmp_path / "fixture",
        mode=ProtectionMode.IDEMPOTENT,
    )
    tools = ToolActivities(
        ToolRuntime(
            database_path=tmp_path / "destination.db",
            workspace_path=tmp_path / "fixture",
            worker_id="continuation-worker",
        )
    )
    environment = await WorkflowEnvironment.start_local()
    async with (
        environment as temporal,
        AgentEnvironment(
            model=FixtureModel(ModelRuntime(worker_id="continuation-worker")),
            model_params=ModelActivityParameters(
                start_to_close_timeout=timedelta(seconds=10),
                streaming_topic="model-events",
            ),
        ) as agent_environment,
    ):
        client = agent_environment.applied_on_client(temporal.client)
        task_queue = f"native-continuation-{uuid.uuid4()}"
        workflow_id = "native-baseline/continued-session"
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[TemporalNativeAgentWorkflow],
            activities=[tools.apply_fixture_change, tools.record_cleanup],
        ):
            handle = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(
                    task="apply after continuation",
                    content="continued fixture\n",
                    approval_required=True,
                    continue_before_agent=True,
                ),
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=15),
            )
            first_run_id = handle.result_run_id
            with pytest.raises(WorkflowContinuedAsNewError) as continued:
                await handle.result(follow_runs=False)
            successor_run_id = continued.value.new_execution_run_id
            status = await handle.query(TemporalNativeAgentWorkflow.status)
            assert status.phase == "awaiting_approval"
            assert status.identity is not None
            owner_capability = status.identity.owner_capability

            stream = WorkflowStreamClient.create(
                client, workflow_id, batch_interval=timedelta(milliseconds=10)
            )
            collector = asyncio.create_task(_collect_events(client, stream, expected_done=True))
            await handle.signal(TemporalNativeAgentWorkflow.approve)
            result = await handle.result()
            events = await asyncio.wait_for(collector, timeout=5)

    assert successor_run_id and successor_run_id != first_run_id
    assert result.owner_capability == owner_capability
    assert {event.owner_capability for event in events} == {owner_capability}
    assert [event.kind for event in events] == [
        "session_started",
        "agent_started",
        "tool_call",
        "tool_output",
        "result_built",
    ]
    snapshot = destination.snapshot()
    assert len(snapshot.attempts) == 1
    assert snapshot.attempts[0].physical_attempt_id.startswith(successor_run_id)
    assert len(snapshot.cleanups) == 1


async def _collect_events(
    client: object,
    stream: WorkflowStreamClient,
    *,
    expected_done: bool,
) -> list[AgentEvent]:
    converter = client.data_converter.payload_converter
    events: list[AgentEvent] = []
    async for item in stream.subscribe(
        [AGENT_EVENTS_TOPIC, STREAM_DONE_TOPIC], result_type=RawValue
    ):
        if item.topic == STREAM_DONE_TOPIC:
            assert converter.from_payload(item.data.payload, bool) is expected_done
            break
        events.append(converter.from_payload(item.data.payload, AgentEvent))
    return events
