from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.workflow_streams import (
    LogicalOutputAcknowledgement,
    LogicalOutputTerminal,
    WorkflowStream,
)
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from .product_contract import (
        Arm,
        OutputAcknowledgement,
        OutputTerminal,
        ProductActivityInput,
        ProductActivityResult,
        ProductWorkflowInput,
        ProductWorkflowResult,
    )
    from .product_manual import validate_manual_acknowledgement

EVENTS_TOPIC = "agent-output-product"
PRODUCT_ACTIVITY_NAME = "publish_product_stream_output"


@workflow.defn
class ProductStreamWorkflow:
    @workflow.init
    def __init__(self, input: ProductWorkflowInput) -> None:
        self.stream = WorkflowStream()
        self._arm = input.arm
        self._terminal: OutputTerminal | None = None
        self._acknowledgement: OutputAcknowledgement | None = None
        self._collection_finished = False

    @workflow.run
    async def run(self, input: ProductWorkflowInput) -> ProductWorkflowResult:
        if input.trial < 1 or input.expected_output != "ABC":
            raise ValueError("Workflow input differs from the frozen schedule")
        result = await workflow.execute_activity(
            PRODUCT_ACTIVITY_NAME,
            ProductActivityInput(
                input.arm,
                input.scenario,
                input.trial,
                input.expected_output,
                f"{workflow.info().workflow_id}/output",
            ),
            result_type=ProductActivityResult,
            start_to_close_timeout=timedelta(seconds=20),
            heartbeat_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100),
                maximum_interval=timedelta(milliseconds=100),
                maximum_attempts=2,
            ),
        )
        self._terminal = result.terminal
        await workflow.wait_condition(
            lambda: self._acknowledgement is not None and self._collection_finished
        )
        acknowledgement = self._acknowledgement
        if acknowledgement is None:
            raise RuntimeError("acknowledgement wait completed without a receipt")
        return ProductWorkflowResult(
            result.full_text,
            result.attempt,
            result.worker_id,
            result.terminal,
            acknowledgement,
        )

    @workflow.update
    async def acknowledge(self, acknowledgement: OutputAcknowledgement) -> None:
        if self._arm is Arm.RAW:
            self._acknowledgement = acknowledgement
            return
        await workflow.wait_condition(lambda: self._terminal is not None)
        terminal = self._terminal
        if terminal is None:
            raise RuntimeError("terminal wait completed without a receipt")
        try:
            if self._arm is Arm.MANUAL:
                validate_manual_acknowledgement(self.stream.get_state(), terminal, acknowledgement)
            else:
                self.stream.validate_logical_output_acknowledgement(
                    _sdk_terminal(terminal), _sdk_acknowledgement(acknowledgement)
                )
        except ValueError as error:
            raise ApplicationError(
                str(error), type="LogicalOutputAcknowledgementRejected"
            ) from error
        self._acknowledgement = acknowledgement

    @workflow.signal
    def finish_collection(self) -> None:
        self._collection_finished = True


def _sdk_terminal(value: OutputTerminal) -> LogicalOutputTerminal:
    return LogicalOutputTerminal(
        value.topic,
        value.logical_output_id,
        value.generation,
        value.terminal_sequence,
        value.chunk_count,
        value.content_sha256,
        value.publisher_id,
    )


def _sdk_acknowledgement(
    value: OutputAcknowledgement,
) -> LogicalOutputAcknowledgement:
    return LogicalOutputAcknowledgement(
        value.topic,
        value.logical_output_id,
        value.generation,
        value.terminal_sequence,
        value.terminal_offset,
        value.chunk_count,
        value.content_sha256,
        value.publisher_id,
    )
