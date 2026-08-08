from __future__ import annotations

import pytest
from pydantic import ValidationError

from temporal_native.contract import TurnIdentity, TurnInput, TurnResult


def test_workflow_mints_stable_nested_operation_ids() -> None:
    identity = TurnIdentity.for_workflow(
        "vendor-baseline/session-7", turn=3, owner_capability="capability-7"
    )

    assert identity.session_id == "vendor-baseline/session-7"
    assert identity.logical_turn_id == "vendor-baseline/session-7/turn/3"
    assert identity.logical_effect_id == "vendor-baseline/session-7/turn/3/effect/1"
    assert identity.generation == 1
    assert identity.owner_capability == "capability-7"


def test_turn_input_rejects_caller_supplied_logical_identity() -> None:
    with pytest.raises(ValidationError):
        TurnInput.model_validate(
            {
                "task": "write the fixture",
                "logical_effect_id": "caller-controlled",
            }
        )


def test_result_must_correlate_every_logical_identity() -> None:
    identity = TurnIdentity.for_workflow("session-1", turn=1, owner_capability="capability-1")
    result = TurnResult(
        **identity.model_dump(),
        artifact_sha256="a" * 64,
        destination_receipt="receipt-1",
    )

    result.require_identity(identity)

    with pytest.raises(ValueError, match="logical identity"):
        result.model_copy(update={"logical_effect_id": "stale"}).require_identity(identity)
