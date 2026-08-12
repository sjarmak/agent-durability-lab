from __future__ import annotations

import base64
from dataclasses import FrozenInstanceError

import pytest

from temporal_coding_agent import (
    AuthorityStatus,
    AuthorizationError,
    CanceledTurnError,
    DestinationCapability,
    EffectOutcome,
    ExecutorAuthorization,
    InvalidTransitionError,
    Lifecycle,
    LogicalIdentity,
    Operation,
    OperationConflictError,
    OperationRequest,
    OwnerCapability,
    ProcessExecutorIdentity,
    ProtocolKernel,
    RevokedAuthorityError,
    StaleAuthorityError,
    StopTarget,
)

CAPABILITY = OwnerCapability.parse(base64.urlsafe_b64encode(b"a" * 32).decode().rstrip("="))
REPLACEMENT_CAPABILITY = OwnerCapability.parse(
    base64.urlsafe_b64encode(b"b" * 32).decode().rstrip("=")
)
REQUEST_HASH = "sha256:" + "2" * 64
OTHER_HASH = "sha256:" + "3" * 64
PROGRESS_HASH = "sha256:" + "8" * 64
RESULT_HASH = "sha256:" + "9" * 64
EXECUTOR = ProcessExecutorIdentity(process_id="pid:412", start_identity="procfs:9001")


def request(name: str, request_hash: str = REQUEST_HASH) -> OperationRequest:
    return OperationRequest(
        operation_id=f"operation:{name}",
        request_hash=request_hash,
        transition_id=f"transition:{name}",
        receipt_id=f"receipt:{name}",
        occurred_at="2026-08-11T12:00:00Z",
    )


def claimed_kernel() -> ProtocolKernel:
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
    return kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )[0]


def running_kernel() -> ProtocolKernel:
    kernel = claimed_kernel()
    kernel = kernel.begin_start(request("begin"), ExecutorAuthorization(1, CAPABILITY))[0]
    return kernel.register(request("register"), ExecutorAuthorization(1, CAPABILITY), EXECUTOR)[0]


@pytest.mark.unit
def test_exact_replay_returns_original_receipt_without_reapplying() -> None:
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
    applied_kernel, accepted = kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )
    replayed_kernel, replayed = applied_kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )

    assert replayed.disposition.value == "replayed"
    assert replayed.receipt is accepted.receipt
    assert replayed.original_transition_id == accepted.transition_id
    assert replayed_kernel is applied_kernel
    assert len(replayed_kernel.records) == 1
    assert CAPABILITY.export_secret() not in repr(replayed_kernel)


@pytest.mark.unit
def test_replay_still_requires_original_authority() -> None:
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
    kernel, _ = kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )

    with pytest.raises(AuthorizationError):
        kernel.claim(
            request("claim"),
            owner_capability=CAPABILITY,
            coordinator_authenticated=False,
        )

    kernel, _ = kernel.begin_start(request("begin"), ExecutorAuthorization(1, CAPABILITY))
    with pytest.raises(RevokedAuthorityError):
        kernel.begin_start(
            request("begin"), ExecutorAuthorization(1, REPLACEMENT_CAPABILITY)
        )


@pytest.mark.unit
def test_reused_operation_id_with_changed_hash_conflicts() -> None:
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
    kernel, _ = kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )

    with pytest.raises(OperationConflictError) as error:
        kernel.claim(
            request("claim", OTHER_HASH),
            owner_capability=CAPABILITY,
            coordinator_authenticated=True,
        )

    assert error.value.rejection.result_type.value == "conflict"
    assert error.value.rejection.request_hash == OTHER_HASH


@pytest.mark.unit
def test_claim_and_replace_replays_bind_capability_and_generation_content() -> None:
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
    claimed, _ = kernel.claim(
        request("claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )
    with pytest.raises(OperationConflictError):
        claimed.claim(
            request("claim"),
            owner_capability=REPLACEMENT_CAPABILITY,
            coordinator_authenticated=True,
        )

    replaced, _ = running_kernel().replace(
        request("replace"), expected_generation=1,
        replacement_capability=REPLACEMENT_CAPABILITY, coordinator_authenticated=True,
    )
    with pytest.raises(OperationConflictError):
        replaced.replace(
            request("replace"), expected_generation=2,
            replacement_capability=CAPABILITY, coordinator_authenticated=True,
        )


@pytest.mark.unit
def test_executor_checks_generation_capability_and_terminal_state() -> None:
    kernel = running_kernel()

    with pytest.raises(StaleAuthorityError):
        kernel.observe_progress(
            request("stale"), ExecutorAuthorization(2, CAPABILITY), PROGRESS_HASH
        )
    with pytest.raises(RevokedAuthorityError):
        kernel.observe_progress(
            request("revoked"), ExecutorAuthorization(1, REPLACEMENT_CAPABILITY), PROGRESS_HASH
        )

    canceled, _ = kernel.cancel(
        request("cancel"),
        expected_generation=1,
        reason_code="operator_canceled",
        coordinator_authenticated=True,
    )
    with pytest.raises(CanceledTurnError):
        canceled.observe_progress(
            request("after-cancel"), ExecutorAuthorization(1, CAPABILITY), PROGRESS_HASH
        )


@pytest.mark.unit
def test_replace_increments_once_and_old_owner_is_stale() -> None:
    kernel = running_kernel()
    registration_request = request("register")
    replaced, result = kernel.replace(
        request("replace"),
        expected_generation=1,
        replacement_capability=REPLACEMENT_CAPABILITY,
        coordinator_authenticated=True,
    )

    assert result.after is not None
    assert result.after.generation == 2
    assert result.after.lifecycle is Lifecycle.STARTING
    assert kernel.state is not None and kernel.state.generation == 1
    with pytest.raises(StaleAuthorityError):
        replaced.attach(request("attach-old"), ExecutorAuthorization(1, CAPABILITY), EXECUTOR)
    unchanged, replay = replaced.register(
        registration_request, ExecutorAuthorization(1, CAPABILITY), EXECUTOR
    )
    assert unchanged is replaced
    assert replay.disposition.value == "replayed"


@pytest.mark.unit
def test_attach_is_complete_self_transition_and_immutable() -> None:
    kernel = running_kernel()
    attached, result = kernel.attach(
        request("attach"), ExecutorAuthorization(1, CAPABILITY), EXECUTOR
    )

    assert result.before is result.after
    assert attached.state is kernel.state
    with pytest.raises(FrozenInstanceError):
        result.receipt.receipt_id = "changed"  # type: ignore[misc]


@pytest.mark.unit
def test_starting_attach_requires_authenticated_discovery() -> None:
    kernel = claimed_kernel().begin_start(
        request("begin-discovery"), ExecutorAuthorization(1, CAPABILITY)
    )[0]
    with pytest.raises(InvalidTransitionError, match="discovery"):
        kernel.attach(
            request("attach-undiscovered"), ExecutorAuthorization(1, CAPABILITY), EXECUTOR
        )
    attached, _ = kernel.attach(
        request("attach-discovered"), ExecutorAuthorization(1, CAPABILITY), EXECUTOR,
        executor_discovered=True,
    )
    assert attached.state is kernel.state


@pytest.mark.unit
def test_illegal_lifecycle_transition_is_rejected() -> None:
    with pytest.raises(InvalidTransitionError):
        running_kernel().begin_start(request("begin-again"), ExecutorAuthorization(1, CAPABILITY))


@pytest.mark.unit
def test_effect_receipt_declares_destination_protocol() -> None:
    kernel, result = running_kernel().publish_effect_receipt(
        request("effect"),
        ExecutorAuthorization(1, CAPABILITY),
        effect_id="effect:tool-call-7",
        destination_namespace="destination:issues",
        destination_capability=DestinationCapability.ATOMIC_IDEMPOTENCY_KEY,
        outcome=EffectOutcome.COMMITTED,
        destination_receipt_id="destination-receipt:7",
    )

    assert kernel.state is result.after
    assert result.receipt.subject.effect_id == "effect:tool-call-7"  # type: ignore[union-attr]


@pytest.mark.unit
def test_stop_receipts_require_revoked_or_superseded_target() -> None:
    kernel = running_kernel()
    target = StopTarget(generation=1, executor_identity=EXECUTOR)

    with pytest.raises(InvalidTransitionError):
        kernel.record_stop_delivery(
            request("stop-current"), target, coordinator_authenticated=True
        )

    replaced, _ = kernel.replace(
        request("replace"),
        expected_generation=1,
        replacement_capability=REPLACEMENT_CAPABILITY,
        coordinator_authenticated=True,
    )
    stopped, delivered = replaced.record_stop_delivery(
        request("stop"), target, coordinator_authenticated=True
    )
    stopped, acknowledged = stopped.acknowledge_stop(
        request("stop-ack"), target, coordinator_authenticated=True
    )

    assert delivered.receipt.subject.delivery_status == "delivered"  # type: ignore[union-attr]
    assert acknowledged.receipt.subject.delivery_status == "acknowledged"  # type: ignore[union-attr]
    assert stopped.state is replaced.state


@pytest.mark.unit
def test_stop_receipts_require_exact_known_target_and_delivery_before_ack() -> None:
    replaced, _ = running_kernel().replace(
        request("replace"),
        expected_generation=1,
        replacement_capability=REPLACEMENT_CAPABILITY,
        coordinator_authenticated=True,
    )
    wrong_target = StopTarget(
        generation=1,
        executor_identity=ProcessExecutorIdentity("pid:999", "procfs:9999"),
    )
    exact_target = StopTarget(generation=1, executor_identity=EXECUTOR)

    with pytest.raises(OperationConflictError):
        replaced.record_stop_delivery(
            request("wrong-stop"), wrong_target, coordinator_authenticated=True
        )
    with pytest.raises(InvalidTransitionError):
        replaced.acknowledge_stop(
            request("early-ack"), exact_target, coordinator_authenticated=True
        )
    with pytest.raises(InvalidTransitionError):
        replaced.record_stop_delivery(
            request("future-stop"),
            StopTarget(generation=3, executor_identity=EXECUTOR),
            coordinator_authenticated=True,
        )


@pytest.mark.unit
def test_all_thirteen_operations_are_exposed() -> None:
    assert {operation.value for operation in Operation} == {
        "claim",
        "begin_start",
        "register",
        "attach",
        "replace",
        "observe_progress",
        "publish_effect_receipt",
        "publish_result",
        "complete",
        "cancel",
        "mark_unresolved",
        "record_stop_delivery",
        "acknowledge_stop",
    }


@pytest.mark.unit
def test_cancel_and_unresolved_revoke_authority() -> None:
    canceled, _ = running_kernel().cancel(
        request("cancel"),
        expected_generation=1,
        reason_code="operator_canceled",
        coordinator_authenticated=True,
    )
    unresolved, _ = running_kernel().mark_unresolved(
        request("unresolved"),
        expected_generation=1,
        reason_code="ambiguous_effect",
        coordinator_authenticated=True,
    )

    assert canceled.state is not None and canceled.state.lifecycle is Lifecycle.CANCELED
    assert unresolved.state is not None and unresolved.state.lifecycle is Lifecycle.UNRESOLVED
    assert canceled.state.authority_status is AuthorityStatus.REVOKED
    assert unresolved.state.authority_status is AuthorityStatus.REVOKED


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["cancel", "unresolved"])
def test_terminal_coordinator_replay_binds_expected_generation(operation: str) -> None:
    kernel = running_kernel()
    if operation == "cancel":
        terminal, _ = kernel.cancel(
            request("terminal-replay"), expected_generation=1,
            reason_code="operator_canceled", coordinator_authenticated=True,
        )
        with pytest.raises(OperationConflictError):
            terminal.cancel(
                request("terminal-replay"), expected_generation=999,
                reason_code="operator_canceled", coordinator_authenticated=True,
            )
    else:
        terminal, _ = kernel.mark_unresolved(
            request("terminal-replay"), expected_generation=1,
            reason_code="ambiguous_effect", coordinator_authenticated=True,
        )
        with pytest.raises(OperationConflictError):
            terminal.mark_unresolved(
                request("terminal-replay"), expected_generation=999,
                reason_code="ambiguous_effect", coordinator_authenticated=True,
            )
