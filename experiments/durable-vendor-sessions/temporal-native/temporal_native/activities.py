"""Temporal Activity exposed to the OpenAI Agents loop as a tool."""

import os
from dataclasses import dataclass
from pathlib import Path

from temporalio import activity

from temporal_native.barrier import BarrierArrival, BarrierClient
from temporal_native.contract import (
    AgentToolInput,
    CleanupRequest,
    DestinationReceipt,
    ToolRequest,
    TurnIdentity,
)
from temporal_native.destination import ControlledDestination

TOOL_EFFECT_COMMITTED = "tool-effect-committed"


@dataclass(frozen=True)
class ToolRuntime:
    """Worker-owned destination and fault-controller configuration."""

    database_path: Path
    workspace_path: Path
    worker_id: str
    barrier_address: str = ""
    barrier_points: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ValueError("worker identity is required")


class ToolActivities:
    """Bound Activity implementation; runtime paths are not model-controlled."""

    def __init__(self, runtime: ToolRuntime) -> None:
        self._runtime = runtime

    @activity.defn(name="apply_fixture_change")
    async def apply_fixture_change(self, tool_input: AgentToolInput) -> str:
        """Apply one deterministic fixture edit to the controlled destination."""

        info = activity.info()
        physical_attempt_id = "/".join(
            (
                info.workflow_run_id or "missing-run",
                info.activity_id,
                "attempt",
                str(info.attempt),
            )
        )
        request = ToolRequest(**tool_input.model_dump(), physical_attempt_id=physical_attempt_id)
        destination = ControlledDestination.open(
            database_path=self._runtime.database_path,
            workspace_path=self._runtime.workspace_path,
        )
        receipt = destination.apply(request)
        if TOOL_EFFECT_COMMITTED in self._runtime.barrier_points:
            await self._arrive(receipt, physical_attempt_id)
        return receipt.model_dump_json()

    @activity.defn(name="record_agent_cleanup")
    async def record_cleanup(self, identity: TurnIdentity, reason: str) -> str:
        """Record retry-safe cleanup outside the deterministic Workflow."""

        info = activity.info()
        physical_attempt_id = "/".join(
            (
                info.workflow_run_id or "missing-run",
                info.activity_id,
                "attempt",
                str(info.attempt),
            )
        )
        destination = ControlledDestination.open(
            database_path=self._runtime.database_path,
            workspace_path=self._runtime.workspace_path,
        )
        cleanup = destination.record_cleanup(
            CleanupRequest(
                **identity.model_dump(),
                physical_attempt_id=physical_attempt_id,
                reason=reason,
            )
        )
        return cleanup.model_dump_json()

    async def _arrive(self, receipt: DestinationReceipt, physical_attempt_id: str) -> None:
        if not self._runtime.barrier_address:
            raise RuntimeError("tool fault barrier is enabled without a controller")
        await BarrierClient(self._runtime.barrier_address).arrive(
            BarrierArrival(
                point=TOOL_EFFECT_COMMITTED,
                session_id=receipt.session_id,
                logical_turn_id=receipt.logical_turn_id,
                logical_effect_id=receipt.logical_effect_id,
                activity_attempt=activity.info().attempt,
                worker_process=f"{self._runtime.worker_id}/pid-{os.getpid()}",
                arrival_token=f"{physical_attempt_id}/barrier/{TOOL_EFFECT_COMMITTED}",
            )
        )
