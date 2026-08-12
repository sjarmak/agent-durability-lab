from __future__ import annotations

from dataclasses import replace

import pytest

from workflow_stream_retry.product_contract import (
    Arm,
    OutputAcknowledgement,
    OutputEvent,
    OutputObservation,
    OutputTerminal,
    ProductScenario,
    ProductTrialRecord,
)
from workflow_stream_retry.product_oracle import audit_product_trial


def _event(
    offset: int,
    *,
    generation: int,
    kind: str,
    text: str = "",
    chunk_index: int | None = None,
) -> OutputObservation:
    return OutputObservation(
        offset,
        OutputEvent(
            logical_output_id="workflow/output",
            generation=generation,
            publisher_id=f"publisher-{generation}",
            activity_attempt=generation,
            worker_id=f"worker-{generation}",
            kind=kind,
            sequence=(
                0 if kind == "begin" else (chunk_index + 1 if chunk_index is not None else 4)
            ),
            chunk_index=chunk_index,
            text=text,
            chunk_count=3 if kind == "complete" else None,
            terminal_sha256="a" * 64 if kind == "complete" else "",
        ),
    )


def _generation(
    start: int,
    generation: int,
    text: str,
    *,
    structural: bool,
    complete: bool,
) -> list[OutputObservation]:
    observations: list[OutputObservation] = []
    offset = start
    if structural:
        observations.append(_event(offset, generation=generation, kind="begin"))
        offset += 1
    for index, chunk in enumerate(text):
        observations.append(
            _event(
                offset,
                generation=generation,
                kind="chunk",
                chunk_index=index,
                text=chunk,
            )
        )
        offset += 1
    if complete:
        observations.append(_event(offset, generation=generation, kind="complete"))
    return observations


def _record(
    arm: Arm,
    scenario: ProductScenario,
    observations: list[OutputObservation],
    *,
    acknowledged_generation: int,
    stale_ack_rejections: int = 0,
) -> ProductTrialRecord:
    final_attempt = 1 if scenario is ProductScenario.UNFAULTED else 2
    final_terminal = next(
        item
        for item in observations
        if item.event.generation == final_attempt and item.event.kind == "complete"
    )
    acknowledged = next(
        item
        for item in observations
        if item.event.generation == acknowledged_generation and item.event.kind == "complete"
    )
    return ProductTrialRecord(
        arm=arm,
        scenario=scenario,
        trial=1,
        workflow_id="workflow",
        run_id="run",
        expected_output="ABC",
        final_attempt=final_attempt,
        final_worker_id=f"worker-{final_attempt}",
        final_terminal=OutputTerminal(
            "events",
            "workflow/output",
            final_attempt,
            final_terminal.event.sequence,
            final_terminal.event.chunk_count or 0,
            final_terminal.event.terminal_sha256,
            final_terminal.event.publisher_id,
        ),
        acknowledgement=OutputAcknowledgement(
            "events",
            "workflow/output",
            acknowledged_generation,
            acknowledged.event.sequence,
            acknowledged.offset,
            acknowledged.event.chunk_count or 0,
            acknowledged.event.terminal_sha256,
            acknowledged.event.publisher_id,
        ),
        stale_ack_rejections=stale_ack_rejections,
        observations=tuple(observations),
        stream_batch_count=1,
        history_event_count=10,
        history_json_bytes=100,
    )


@pytest.mark.parametrize("arm", [Arm.MANUAL, Arm.PRODUCT])
def test_protected_arms_replace_a_durable_partial_prefix(arm: Arm) -> None:
    first = _generation(0, 1, "AB", structural=True, complete=False)
    replacement = _generation(len(first), 2, "ABC", structural=True, complete=True)
    record = _record(
        arm,
        ProductScenario.POST_FLUSH_PREFIX,
        first + replacement,
        acknowledged_generation=2,
    )

    verdict = audit_product_trial(record)

    assert verdict.valid
    assert verdict.raw_concatenation == "ABABC"
    assert verdict.reconstructed_output == "ABC"
    assert not verdict.stale_ack_accepted


def test_raw_negative_control_duplicates_a_durable_partial_prefix() -> None:
    first = _generation(0, 1, "AB", structural=False, complete=False)
    replacement = _generation(len(first), 2, "ABC", structural=False, complete=True)
    record = _record(
        Arm.RAW,
        ProductScenario.POST_FLUSH_PREFIX,
        first + replacement,
        acknowledged_generation=2,
    )

    verdict = audit_product_trial(record)

    assert verdict.valid
    assert verdict.raw_concatenation == "ABABC"
    assert verdict.reconstructed_output == "ABABC"
    assert verdict.duplicate_output


@pytest.mark.parametrize("arm", [Arm.MANUAL, Arm.PRODUCT])
def test_protected_arms_reject_terminal_from_attempt_that_later_dies(
    arm: Arm,
) -> None:
    first = _generation(0, 1, "ABC", structural=True, complete=True)
    replacement = _generation(len(first), 2, "ABC", structural=True, complete=True)
    record = _record(
        arm,
        ProductScenario.TERMINAL_BEFORE_ACK,
        first + replacement,
        acknowledged_generation=2,
        stale_ack_rejections=1,
    )

    verdict = audit_product_trial(record)

    assert verdict.valid
    assert verdict.reconstructed_output == "ABC"
    assert verdict.stale_ack_rejections == 1
    assert not verdict.stale_ack_accepted


def test_raw_negative_control_accepts_stale_terminal_acknowledgement() -> None:
    first = _generation(0, 1, "ABC", structural=False, complete=True)
    replacement = _generation(len(first), 2, "ABC", structural=False, complete=True)
    record = _record(
        Arm.RAW,
        ProductScenario.TERMINAL_BEFORE_ACK,
        first + replacement,
        acknowledged_generation=1,
    )

    verdict = audit_product_trial(record)

    assert verdict.valid
    assert verdict.stale_ack_accepted
    assert verdict.duplicate_output


def test_product_oracle_rejects_wrong_ack_offset_and_sequence_gap() -> None:
    observations = _generation(0, 1, "ABC", structural=True, complete=True)
    record = _record(
        Arm.PRODUCT,
        ProductScenario.UNFAULTED,
        observations,
        acknowledged_generation=1,
    )
    with pytest.raises(ValueError, match="acknowledgement"):
        audit_product_trial(
            replace(
                record,
                acknowledgement=replace(record.acknowledgement, terminal_offset=1),
            )
        )

    broken = list(observations)
    broken[2] = replace(
        broken[2],
        event=replace(broken[2].event, sequence=9),
    )
    with pytest.raises(ValueError, match="sequence"):
        audit_product_trial(replace(record, observations=tuple(broken)))

    with pytest.raises(ValueError, match="history metrics"):
        audit_product_trial(replace(record, history_json_bytes=0))
