from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import temporalio.contrib.workflow_streams as workflow_streams
from temporalio.testing import WorkflowEnvironment

if not hasattr(workflow_streams, "LogicalOutputReconstructor"):
    pytest.skip(
        "requires the pinned retry-aware sdk-python candidate patch",
        allow_module_level=True,
    )

from workflow_stream_retry.barrier import BarrierServer
from workflow_stream_retry.product_contract import Arm, ProductScenario
from workflow_stream_retry.product_runner import run_product_trial


async def test_real_worker_loss_distinguishes_raw_and_protected_arms(
    tmp_path: Path,
) -> None:
    project_root = Path(__file__).parents[1]
    environment = await WorkflowEnvironment.start_local()
    captures = []
    async with (
        environment as temporal,
        BarrierServer(tmp_path / "product-barrier.sock") as barrier,
    ):
        for arm in Arm:
            for scenario in ProductScenario:
                captures.append(
                    await run_product_trial(
                        client=temporal.client,
                        project_root=project_root,
                        barrier=barrier,
                        arm=arm,
                        scenario=scenario,
                        trial=1,
                        run_label=f"product-live-{uuid.uuid4()}",
                    )
                )

    assert all(capture.verdict.valid for capture in captures)
    assert {
        (capture.record.arm, capture.record.scenario)
        for capture in captures
        if capture.verdict.duplicate_output
    } == {
        (Arm.RAW, ProductScenario.POST_FLUSH_PREFIX),
        (Arm.RAW, ProductScenario.TERMINAL_BEFORE_ACK),
    }
    assert {
        (capture.record.arm, capture.record.scenario)
        for capture in captures
        if capture.verdict.stale_ack_accepted
    } == {(Arm.RAW, ProductScenario.TERMINAL_BEFORE_ACK)}
