from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.testing import WorkflowEnvironment

from temporal_native.contract import TurnInput, TurnResult
from temporal_native.destination import ControlledDestination, ProtectionMode
from temporal_native.faults import DropStartAcknowledgement, StartAcknowledgementLost
from temporal_native.worker_process import launch_worker, stop_worker
from temporal_native.workflow import TemporalNativeAgentWorkflow


async def test_stable_workflow_id_recovers_after_server_ack_is_discarded(
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
        task_queue = f"native-start-ack-{uuid.uuid4()}"
        worker = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="start-ack-worker",
        )
        workflow_id = "native-baseline/start-ack-loss"
        drop_ack = DropStartAcknowledgement()
        fault_config = client.config()
        fault_config["interceptors"] = [*fault_config["interceptors"], drop_ack]
        fault_client = Client(**fault_config)
        try:
            with pytest.raises(StartAcknowledgementLost):
                await fault_client.start_workflow(
                    TemporalNativeAgentWorkflow.run,
                    TurnInput(task="apply fixture", content="ack-loss fixture\n"),
                    id=workflow_id,
                    task_queue=task_queue,
                    execution_timeout=timedelta(seconds=30),
                    request_id="start-request-1",
                )
            observation = drop_ack.observation
            assert observation.workflow_id == workflow_id
            assert observation.request_id == "start-request-1"

            recovered = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(task="apply fixture", content="ack-loss fixture\n"),
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                request_id="recovery-request-1",
            )
            result = await asyncio.wait_for(recovered.result(), timeout=15)
            history = [event async for event in recovered.fetch_history_events()]
        finally:
            await stop_worker(worker, kill=False)

    assert isinstance(result, TurnResult)
    assert recovered.first_execution_run_id == observation.workflow_run_id
    assert (
        sum(event.HasField("workflow_execution_started_event_attributes") for event in history) == 1
    )
    assert len(destination.snapshot().attempts) == 1
