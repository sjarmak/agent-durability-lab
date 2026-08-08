"""Exact client-side fault boundaries used by the live experiment harness."""

from __future__ import annotations

from typing import Any

from temporalio.client import (
    Interceptor,
    OutboundInterceptor,
    StartWorkflowInput,
    WorkflowHandle,
)

from temporal_native.contract import StrictModel


class StartAcknowledgementLost(RuntimeError):
    """The service accepted StartWorkflow, but the application saw no handle."""


class StartAckObservation(StrictModel):
    """Server acknowledgement captured immediately before it is discarded."""

    workflow_id: str
    workflow_run_id: str
    request_id: str


class DropStartAcknowledgement(Interceptor):
    """One-use interceptor that discards a successful StartWorkflow response."""

    def __init__(self) -> None:
        self._observation: StartAckObservation | None = None

    @property
    def observation(self) -> StartAckObservation:
        if self._observation is None:
            raise RuntimeError("start acknowledgement has not been observed")
        return self._observation

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        return _DropStartAcknowledgementOutbound(next, self)

    def _record(self, input: StartWorkflowInput, handle: WorkflowHandle[Any, Any]) -> None:
        if self._observation is not None:
            raise RuntimeError("start acknowledgement fault is one-use")
        run_id = handle.first_execution_run_id
        if not run_id:
            raise RuntimeError("successful StartWorkflow response lacks a run identity")
        self._observation = StartAckObservation(
            workflow_id=input.id,
            workflow_run_id=run_id,
            request_id=input.request_id or "",
        )


class _DropStartAcknowledgementOutbound(OutboundInterceptor):
    def __init__(
        self,
        next: OutboundInterceptor,
        fault: DropStartAcknowledgement,
    ) -> None:
        super().__init__(next)
        self._fault = fault

    async def start_workflow(self, input: StartWorkflowInput) -> WorkflowHandle[Any, Any]:
        handle = await self.next.start_workflow(input)
        self._fault._record(input, handle)
        raise StartAcknowledgementLost(
            "StartWorkflow acknowledgement discarded before application registration"
        )
