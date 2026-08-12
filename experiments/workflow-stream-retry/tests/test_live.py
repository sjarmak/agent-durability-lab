from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment

from workflow_stream_retry.barrier import BarrierServer
from workflow_stream_retry.contract import Scenario
from workflow_stream_retry.runner import run_trial


@pytest.mark.parametrize("scenario", list(Scenario))
async def test_real_service_worker_loss_reconstructs_stream_by_retry_identity(
    tmp_path: Path, scenario: Scenario
) -> None:
    project_root = Path(__file__).parents[1]
    environment = await WorkflowEnvironment.start_local()
    async with environment as temporal, BarrierServer(tmp_path / "barrier.sock") as barrier:
        capture = await run_trial(
            client=temporal.client,
            project_root=project_root,
            barrier=barrier,
            scenario=scenario,
            trial=1,
            run_label=f"workflow-stream-live-{uuid.uuid4()}",
        )
    assert capture.verdict.valid
    assert capture.verdict.retry_aware_output == "ABC"
    assert len(capture.worker_processes) == (1 if scenario is Scenario.UNFAULTED else 2)
