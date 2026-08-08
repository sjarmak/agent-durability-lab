from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.openai_agents import OpenAIAgentsPlugin
from temporalio.exceptions import CancelledError, TerminatedError
from temporalio.testing import WorkflowEnvironment

from temporal_native.contract import TurnInput
from temporal_native.destination import ControlledDestination, ProtectionMode
from temporal_native.worker_process import launch_worker, stop_worker
from temporal_native.workflow import TemporalNativeAgentWorkflow


async def test_graceful_cancel_records_cleanup_but_terminate_cannot(
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
        task_queue = f"native-cancellation-{uuid.uuid4()}"
        worker = await launch_worker(
            project_root=project_root,
            address=client.service_client.config.target_host,
            task_queue=task_queue,
            database_path=database_path,
            workspace_path=workspace_path,
            worker_id="cancellation-worker",
        )
        try:
            cancel_handle = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(
                    task="wait for approval",
                    content="cancel fixture\n",
                    approval_required=True,
                ),
                id="native-baseline/graceful-cancel",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            cancel_status = await cancel_handle.query(TemporalNativeAgentWorkflow.status)
            assert cancel_status.phase == "awaiting_approval"
            await cancel_handle.cancel(reason="operator requested graceful stop")
            with pytest.raises(WorkflowFailureError) as canceled:
                await asyncio.wait_for(cancel_handle.result(), timeout=10)
            assert isinstance(canceled.value.cause, CancelledError)

            terminate_handle = await client.start_workflow(
                TemporalNativeAgentWorkflow.run,
                TurnInput(
                    task="wait for approval",
                    content="terminate fixture\n",
                    approval_required=True,
                ),
                id="native-baseline/forced-terminate",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            terminate_status = await terminate_handle.query(TemporalNativeAgentWorkflow.status)
            assert terminate_status.phase == "awaiting_approval"
            await terminate_handle.terminate(reason="forced termination control")
            with pytest.raises(WorkflowFailureError) as terminated:
                await asyncio.wait_for(terminate_handle.result(), timeout=10)
            assert isinstance(terminated.value.cause, TerminatedError)
        finally:
            await stop_worker(worker, kill=False)

    snapshot = destination.snapshot()
    assert snapshot.attempts == ()
    assert [cleanup.session_id for cleanup in snapshot.cleanups] == [
        "native-baseline/graceful-cancel"
    ]
