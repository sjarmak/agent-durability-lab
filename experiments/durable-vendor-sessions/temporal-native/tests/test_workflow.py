from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path

from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from temporalio.contrib.openai_agents.testing import AgentEnvironment
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from temporal_native.activities import ToolActivities, ToolRuntime
from temporal_native.contract import TurnInput, TurnResult
from temporal_native.destination import ControlledDestination, ProtectionMode
from temporal_native.model import FixtureModel, ModelRuntime
from temporal_native.workflow import TemporalNativeAgentWorkflow


async def test_agent_loop_correlates_model_tool_destination_and_result(tmp_path: Path) -> None:
    destination = ControlledDestination.create(
        database_path=tmp_path / "destination.db",
        workspace_path=tmp_path / "fixture",
        mode=ProtectionMode.IDEMPOTENT,
    )
    tools = ToolActivities(
        ToolRuntime(
            database_path=tmp_path / "destination.db",
            workspace_path=tmp_path / "fixture",
            worker_id="integration-worker",
        )
    )

    environment = await WorkflowEnvironment.start_time_skipping()
    async with (
        environment as temporal,
        AgentEnvironment(
            model=FixtureModel(ModelRuntime(worker_id="integration-worker")),
            model_params=ModelActivityParameters(
                start_to_close_timeout=timedelta(seconds=10),
                streaming_topic="model-events",
            ),
        ) as agent_environment,
    ):
        client = agent_environment.applied_on_client(temporal.client)
        task_queue = f"native-agent-{uuid.uuid4()}"
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[TemporalNativeAgentWorkflow],
            activities=[tools.apply_fixture_change, tools.record_cleanup],
        ):
            handle = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(task="apply fixture", content="durable fixture\n"),
                id="native-baseline/session-1",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=10),
            )
            result = await handle.result()
            history = await handle.fetch_history()

    await Replayer(
        workflows=[TemporalNativeAgentWorkflow],
        plugins=[
            OpenAIAgentsPlugin(
                register_activities=False,
                add_temporal_spans=False,
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=10),
                    streaming_topic="model-events",
                ),
            )
        ],
    ).replay_workflow(history)

    assert isinstance(result, TurnResult)
    assert result.session_id == "native-baseline/session-1"
    assert result.logical_effect_id == "native-baseline/session-1/turn/1/effect/1"
    snapshot = destination.snapshot()
    assert len(snapshot.attempts) == 1
    assert snapshot.attempts[0].logical_effect_id == result.logical_effect_id
    assert snapshot.attempts[0].artifact_sha256 == result.artifact_sha256
    assert snapshot.attempts[0].receipt_id == result.destination_receipt


async def test_approval_signal_gates_model_and_tool_calls(tmp_path: Path) -> None:
    destination = ControlledDestination.create(
        database_path=tmp_path / "destination.db",
        workspace_path=tmp_path / "fixture",
        mode=ProtectionMode.IDEMPOTENT,
    )
    tools = ToolActivities(
        ToolRuntime(
            database_path=tmp_path / "destination.db",
            workspace_path=tmp_path / "fixture",
            worker_id="approval-worker",
        )
    )

    environment = await WorkflowEnvironment.start_time_skipping()
    async with (
        environment as temporal,
        AgentEnvironment(
            model=FixtureModel(ModelRuntime(worker_id="approval-worker")),
            model_params=ModelActivityParameters(
                start_to_close_timeout=timedelta(seconds=10),
                streaming_topic="model-events",
            ),
        ) as agent_environment,
    ):
        client = agent_environment.applied_on_client(temporal.client)
        task_queue = f"native-agent-{uuid.uuid4()}"
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[TemporalNativeAgentWorkflow],
            activities=[tools.apply_fixture_change, tools.record_cleanup],
        ):
            handle = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(
                    task="apply approved fixture",
                    content="approved fixture\n",
                    approval_required=True,
                ),
                id="native-baseline/approval-session",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=10),
            )
            status = await handle.query(TemporalNativeAgentWorkflow.status)
            assert status.phase == "awaiting_approval"
            assert destination.snapshot().attempts == ()

            await handle.signal(TemporalNativeAgentWorkflow.approve)
            result = await handle.result()

    assert result.session_id == "native-baseline/approval-session"
    assert len(destination.snapshot().attempts) == 1
