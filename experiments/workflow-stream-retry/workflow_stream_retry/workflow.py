from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from .contract import ActivityInput, ActivityResult, WorkflowInput, WorkflowResult

EVENTS_TOPIC = "agent-output"
ACTIVITY_NAME = "publish_stream_output"


@workflow.defn
class StreamRetryWorkflow:
    @workflow.init
    def __init__(self, input: WorkflowInput) -> None:
        self.stream = WorkflowStream()
        self._acknowledged_offset = -1

    @workflow.run
    async def run(self, input: WorkflowInput) -> WorkflowResult:
        if input.trial < 1 or not input.expected_output:
            raise ValueError("workflow input is invalid")
        activity_result = await workflow.execute_activity(
            ACTIVITY_NAME,
            ActivityInput(
                scenario=input.scenario,
                trial=input.trial,
                expected_output=input.expected_output,
                logical_output_id=f"{workflow.info().workflow_id}/output",
            ),
            result_type=ActivityResult,
            start_to_close_timeout=timedelta(seconds=20),
            heartbeat_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100),
                maximum_interval=timedelta(milliseconds=100),
                maximum_attempts=2,
            ),
        )
        await workflow.wait_condition(lambda: self._acknowledged_offset >= 0)
        return WorkflowResult(
            full_text=activity_result.full_text,
            final_attempt=activity_result.attempt,
            final_worker_id=activity_result.worker_id,
            acknowledged_offset=self._acknowledged_offset,
        )

    @workflow.signal
    def acknowledge(self, offset: int) -> None:
        if offset < 0:
            raise ValueError("acknowledgement offset must be nonnegative")
        self._acknowledged_offset = max(self._acknowledged_offset, offset)
