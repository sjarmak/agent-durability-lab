from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workflow_stream_retry.barrier import BarrierArrival, BarrierClient, BarrierServer
from workflow_stream_retry.product_activity import (
    ProductActivityRuntime,
    ProductPublisherActivities,
)
from workflow_stream_retry.product_contract import (
    Arm,
    ProductScenario,
    ProductWorkflowInput,
)
from workflow_stream_retry.product_runner import _collect
from workflow_stream_retry.product_workflow import ProductStreamWorkflow


@pytest.mark.parametrize("arm", list(Arm))
@pytest.mark.parametrize("scenario", list(ProductScenario))
async def test_released_fault_barrier_exercises_every_publisher_path(
    tmp_path: Path,
    arm: Arm,
    scenario: ProductScenario,
) -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    async with (
        environment as temporal,
        BarrierServer(tmp_path / "product-activity.sock") as barrier,
    ):
        workflow_id = f"product-activity-{uuid.uuid4()}"
        worker_id = "product-activity-worker"
        runtime_barrier = BarrierClient(barrier.socket_path, barrier.credential)
        activities = ProductPublisherActivities(ProductActivityRuntime(worker_id, runtime_barrier))
        if scenario is not ProductScenario.UNFAULTED:
            expected = BarrierArrival(scenario.value, workflow_id, 1, worker_id)
            barrier.expect(expected)
        task_queue = f"product-activity-{uuid.uuid4()}"
        async with Worker(
            temporal.client,
            task_queue=task_queue,
            workflows=[ProductStreamWorkflow],
            activities=[activities.publish],
            identity=worker_id,
        ):
            handle = await temporal.client.start_workflow(
                ProductStreamWorkflow.run,
                ProductWorkflowInput(arm, scenario, 1, "ABC"),
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            fault_committed = asyncio.Event()
            collection = asyncio.create_task(
                _collect(
                    temporal.client,
                    handle,
                    workflow_id,
                    arm,
                    scenario,
                    fault_committed,
                )
            )
            if scenario is not ProductScenario.UNFAULTED:
                assert await barrier.next_arrival() == expected
                barrier.release(expected)
                fault_committed.set()
                await temporal.sleep(timedelta(seconds=1))
            observed = await asyncio.wait_for(collection, timeout=5)
            result = await handle.result()

    assert result.final_attempt == (1 if scenario is ProductScenario.UNFAULTED else 2)
    expected_raw = {
        ProductScenario.UNFAULTED: "ABC",
        ProductScenario.PRE_FLUSH_LOSS: "ABABC",
        ProductScenario.POST_FLUSH_PREFIX: "ABABC",
        ProductScenario.TERMINAL_BEFORE_ACK: "ABCABC",
    }
    assert observed.reconstructed_output == (expected_raw[scenario] if arm is Arm.RAW else "ABC")
    assert observed.stale_ack_rejections == (
        1 if arm is not Arm.RAW and scenario is ProductScenario.TERMINAL_BEFORE_ACK else 0
    )
