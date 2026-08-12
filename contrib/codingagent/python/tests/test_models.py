from __future__ import annotations

import json
import pickle
from dataclasses import FrozenInstanceError, asdict

import pytest

from temporal_coding_agent import (
    AuthorityStatus,
    ExecutorAuthorization,
    Lifecycle,
    LogicalIdentity,
    OperationRequest,
    OwnerCapability,
    TurnState,
    capability_digest,
    mint_owner_capability,
)

DIGEST = "sha256:" + "1" * 64
REQUEST_HASH = "sha256:" + "2" * 64


@pytest.mark.unit
def test_models_are_immutable_and_validate_stable_identity() -> None:
    identity = LogicalIdentity(session_id="session:reviewer", turn_id="turn:42")

    with pytest.raises(FrozenInstanceError):
        identity.turn_id = "turn:43"  # type: ignore[misc]
    with pytest.raises(ValueError, match="session_id"):
        LogicalIdentity(session_id="../secret", turn_id="turn:42")


@pytest.mark.unit
def test_state_rejects_terminal_current_authority() -> None:
    with pytest.raises(ValueError, match="terminal lifecycle"):
        TurnState(
            lifecycle=Lifecycle.SUCCEEDED,
            generation=2,
            owner_capability_digest=DIGEST,
            authority_status=AuthorityStatus.CURRENT,
        )


@pytest.mark.unit
@pytest.mark.parametrize("occurred_at", ["2026-99-99T99:99:99Z", "2026-08-11 12:00:00Z"])
def test_request_requires_real_utc_instant_and_digest(occurred_at: str) -> None:
    with pytest.raises(ValueError, match="occurred_at"):
        OperationRequest(
            operation_id="operation:start",
            request_hash=REQUEST_HASH,
            transition_id="transition:start",
            receipt_id="receipt:start",
            occurred_at=occurred_at,
        )
    with pytest.raises(ValueError, match="request_hash"):
        OperationRequest(
            operation_id="operation:start",
            request_hash="not-a-digest",
            transition_id="transition:start",
            receipt_id="receipt:start",
            occurred_at="2026-08-11T12:00:00Z",
        )


@pytest.mark.unit
def test_capability_is_high_entropy_and_digest_only_model() -> None:
    capability = mint_owner_capability()
    authorization = ExecutorAuthorization(generation=1, capability=capability)

	# Export is intentionally explicit; routine formatting and serialization stay redacted.
    assert len(capability.export_secret()) >= 43
    assert capability_digest(capability).startswith("sha256:")
    assert capability.export_secret() not in repr(authorization)
    assert isinstance(asdict(authorization)["capability"], OwnerCapability)
    with pytest.raises(TypeError):
        json.dumps(asdict(authorization))
    with pytest.raises(TypeError):
        pickle.dumps(authorization)


@pytest.mark.unit
def test_capability_parser_requires_canonical_256_bit_secret() -> None:
    capability = mint_owner_capability()
    restored = OwnerCapability.parse(capability.export_secret())
    assert restored.matches_digest(capability.digest())
    for value in ("", "x" * 32, "not-base64"):
        with pytest.raises(ValueError):
            OwnerCapability.parse(value)
    with pytest.raises(ValueError, match="opaque"):
        ExecutorAuthorization(generation=1, capability="x" * 32)  # type: ignore[arg-type]
