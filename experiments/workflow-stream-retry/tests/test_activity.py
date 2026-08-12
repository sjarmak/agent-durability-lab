from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workflow_stream_retry.activity import ActivityRuntime, PublisherActivities
from workflow_stream_retry.barrier import BarrierArrival, BarrierClient, BarrierServer
from workflow_stream_retry.contract import Scenario, StreamEvent, WorkflowInput
from workflow_stream_retry.workflow import EVENTS_TOPIC, StreamRetryWorkflow


@pytest.mark.parametrize("scenario", [Scenario.PRE_FLUSH_LOSS, Scenario.POST_FLUSH_DUPLICATE])
async def test_released_fault_barrier_retries_with_explicit_marker(
    tmp_path: Path, scenario: Scenario
) -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    async with environment as temporal, BarrierServer(tmp_path / "barrier.sock") as barrier:
        workflow_id = f"activity-unit-{uuid.uuid4()}"
        worker_id = "activity-unit-worker"
        expected = BarrierArrival(scenario.value, workflow_id, 1, worker_id)
        barrier.expect(expected)
        activities = PublisherActivities(
            ActivityRuntime(worker_id, BarrierClient(barrier.socket_path, barrier.credential))
        )
        task_queue = f"activity-unit-{uuid.uuid4()}"
        async with Worker(
            temporal.client,
            task_queue=task_queue,
            workflows=[StreamRetryWorkflow],
            activities=[activities.publish],
            identity=worker_id,
        ):
            handle = await temporal.client.start_workflow(
                StreamRetryWorkflow.run,
                WorkflowInput(scenario, 1, "ABC"),
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=20),
            )
            release = asyncio.create_task(_release(barrier, expected))
            stream = WorkflowStreamClient.create(temporal.client, workflow_id)
            kinds = []
            async for item in stream.subscribe(EVENTS_TOPIC, result_type=StreamEvent):
                kinds.append(item.data.kind)
                if item.data.kind == "complete":
                    await handle.signal(StreamRetryWorkflow.acknowledge, item.offset)
                    break
            result = await handle.result()
            await release
    assert result.final_attempt == 2
    assert kinds.count("retry") == 1


async def _release(barrier: BarrierServer, expected: BarrierArrival) -> None:
    assert await barrier.next_arrival() == expected
    barrier.release(expected)
