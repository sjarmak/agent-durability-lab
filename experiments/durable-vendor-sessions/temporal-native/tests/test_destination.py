from __future__ import annotations

import hashlib
from datetime import UTC
from pathlib import Path

import pytest

from temporal_native.contract import ToolRequest, TurnIdentity
from temporal_native.destination import ControlledDestination, ProtectionMode


def request_for(identity: TurnIdentity, physical_attempt_id: str) -> ToolRequest:
    return ToolRequest(
        **identity.model_dump(),
        physical_attempt_id=physical_attempt_id,
        relative_path="result.txt",
        content="durable fixture\n",
    )


@pytest.mark.parametrize(
    ("mode", "expected_applied"),
    [(ProtectionMode.UNSAFE, 2), (ProtectionMode.IDEMPOTENT, 1)],
)
def test_destination_distinguishes_delivery_attempts_from_applied_effects(
    tmp_path: Path,
    mode: ProtectionMode,
    expected_applied: int,
) -> None:
    identity = TurnIdentity.for_workflow("session-1", turn=1, owner_capability="capability-1")
    destination = ControlledDestination.create(
        database_path=tmp_path / "destination.db",
        workspace_path=tmp_path / "fixture",
        mode=mode,
    )

    first = destination.apply(request_for(identity, "attempt-1"))
    second = destination.apply(request_for(identity, "attempt-2"))

    snapshot = destination.snapshot()
    assert len(snapshot.attempts) == 2
    assert sum(attempt.applied for attempt in snapshot.attempts) == expected_applied
    assert all(attempt.observed_at.tzinfo == UTC for attempt in snapshot.attempts)
    assert snapshot.attempts[0].observed_at < snapshot.attempts[1].observed_at
    assert first.artifact_sha256 == second.artifact_sha256
    assert (tmp_path / "fixture" / "result.txt").read_text() == "durable fixture\n"
    assert first.artifact_sha256 == hashlib.sha256(b"durable fixture\n").hexdigest()


def test_destination_rejects_a_physical_attempt_id_reused_for_another_effect(
    tmp_path: Path,
) -> None:
    first_identity = TurnIdentity.for_workflow("session-1", turn=1, owner_capability="capability-1")
    second_identity = TurnIdentity.for_workflow(
        "session-1", turn=2, owner_capability="capability-2"
    )
    destination = ControlledDestination.create(
        database_path=tmp_path / "destination.db",
        workspace_path=tmp_path / "fixture",
        mode=ProtectionMode.IDEMPOTENT,
    )
    destination.apply(request_for(first_identity, "attempt-reused"))

    with pytest.raises(ValueError, match="physical attempt"):
        destination.apply(request_for(second_identity, "attempt-reused"))
