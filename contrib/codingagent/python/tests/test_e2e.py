from __future__ import annotations

import base64

import pytest

from temporal_coding_agent import (
    AuthorityStatus,
    DestinationCapability,
    EffectOutcome,
    ExecutorAuthorization,
    Lifecycle,
    LogicalIdentity,
    OperationRequest,
    OwnerCapability,
    ProcessExecutorIdentity,
    ProtocolKernel,
)

CAPABILITY = OwnerCapability.parse(base64.urlsafe_b64encode(b"a" * 32).decode().rstrip("="))
REQUEST_HASHES = [f"sha256:{number:064x}" for number in range(1, 10)]


def request(index: int, name: str) -> OperationRequest:
    return OperationRequest(
        operation_id=f"operation:{name}",
        request_hash=REQUEST_HASHES[index],
        transition_id=f"transition:{name}",
        receipt_id=f"receipt:{name}",
        occurred_at=f"2026-08-11T12:00:0{index}Z",
    )


@pytest.mark.e2e
def test_claim_to_effect_to_completion_flow_with_retry() -> None:
    executor = ProcessExecutorIdentity("pid:412", "procfs:9001")
    authorization = ExecutorAuthorization(1, CAPABILITY)
    kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))

    kernel, _ = kernel.claim(
        request(0, "claim"), owner_capability=CAPABILITY, coordinator_authenticated=True
    )
    kernel, _ = kernel.begin_start(request(1, "start"), authorization)
    kernel, registration = kernel.register(request(2, "register"), authorization, executor)
    replayed_kernel, replay = kernel.register(request(2, "register"), authorization, executor)
    assert replayed_kernel is kernel
    assert replay.receipt is registration.receipt

    kernel, _ = kernel.observe_progress(
        request(3, "progress"), authorization, "sha256:" + "8" * 64
    )
    kernel, _ = kernel.publish_effect_receipt(
        request(4, "effect"),
        authorization,
        effect_id="effect:tool-call-7",
        destination_namespace="destination:issues",
        destination_capability=DestinationCapability.ATOMIC_IDEMPOTENCY_KEY,
        outcome=EffectOutcome.COMMITTED,
        destination_receipt_id="destination-receipt:7",
    )
    kernel, result = kernel.publish_result(
        request(5, "result"),
        authorization,
        result_hash="sha256:" + "9" * 64,
        system_of_record_check="check:destination-state",
    )
    kernel, completion = kernel.complete(
        request(6, "complete"), authorization, result_receipt_id=result.receipt.receipt_id
    )

    assert kernel.state is not None
    assert kernel.state.lifecycle is Lifecycle.SUCCEEDED
    assert kernel.state.authority_status is AuthorityStatus.REVOKED
    assert completion.receipt.subject.result_receipt_id == result.receipt.receipt_id  # type: ignore[union-attr]
    assert len(kernel.records) == 7
