"""Typed identities and payloads shared by the Workflow, model, and tool."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    """Immutable, fail-closed payload base."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class TurnInput(StrictModel):
    """Caller-controlled task data; logical identities are deliberately absent."""

    task: str = Field(min_length=1)
    relative_path: str = "result.txt"
    content: str = Field(min_length=1)
    approval_required: bool = False
    continue_before_agent: bool = False
    hold_result: bool = False


class TurnIdentity(StrictModel):
    """Stable application identity minted inside the Workflow."""

    session_id: str = Field(min_length=1)
    logical_turn_id: str = Field(min_length=1)
    logical_effect_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    owner_capability: str = Field(min_length=1)

    @classmethod
    def for_workflow(
        cls,
        workflow_id: str,
        *,
        turn: int,
        owner_capability: str,
        generation: int = 1,
    ) -> Self:
        """Construct nested logical IDs from a stable Workflow ID."""

        if not workflow_id or turn < 1:
            raise ValueError("workflow ID and positive turn are required")
        logical_turn_id = f"{workflow_id}/turn/{turn}"
        return cls(
            session_id=workflow_id,
            logical_turn_id=logical_turn_id,
            logical_effect_id=f"{logical_turn_id}/effect/1",
            generation=generation,
            owner_capability=owner_capability,
        )


class AgentEvent(TurnIdentity):
    """Flattened, serializable event delivered to external observers."""

    kind: str = Field(min_length=1)
    detail: str = Field(min_length=1)


class AgentTurnEnvelope(TurnIdentity):
    """Workflow-authored context supplied to the deterministic fixture model."""

    task: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    content: str = Field(min_length=1)


class AgentToolInput(TurnIdentity):
    """Logical tool request selected by the model."""

    relative_path: str = Field(min_length=1)
    content: str = Field(min_length=1)


class ToolRequest(AgentToolInput):
    """One physical delivery attempt for a stable logical tool effect."""

    physical_attempt_id: str = Field(min_length=1)


class CleanupRequest(TurnIdentity):
    """One physical delivery of cleanup for a stable session owner."""

    physical_attempt_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class DestinationReceipt(TurnIdentity):
    """Destination observation returned to the agent tool loop."""

    destination_id: str = Field(min_length=1)
    physical_attempt_id: str = Field(min_length=1)
    receipt_id: str = Field(min_length=1)
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    applied: bool


class TurnResult(TurnIdentity):
    """Typed model result correlated with independently observed state."""

    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    destination_receipt: str = Field(min_length=1)

    def require_identity(self, expected: TurnIdentity) -> None:
        """Reject a model result that changed any stable logical identity."""

        observed = TurnIdentity(
            session_id=self.session_id,
            logical_turn_id=self.logical_turn_id,
            logical_effect_id=self.logical_effect_id,
            generation=self.generation,
            owner_capability=self.owner_capability,
        )
        if observed != expected:
            raise ValueError("result logical identity does not match Workflow identity")

    @model_validator(mode="after")
    def receipt_is_not_an_identity(self) -> Self:
        if self.destination_receipt in {
            self.session_id,
            self.logical_turn_id,
            self.logical_effect_id,
            self.owner_capability,
        }:
            raise ValueError("destination receipt must be an independent identity")
        return self
