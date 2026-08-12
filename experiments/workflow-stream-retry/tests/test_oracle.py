from __future__ import annotations

from dataclasses import replace

import pytest

from workflow_stream_retry.contract import (
    PublishedBatch,
    Scenario,
    StreamEvent,
    StreamObservation,
    TrialRecord,
)
from workflow_stream_retry.oracle import audit_trial, reconstruct_naive, reconstruct_retry_aware


def _event(offset: int, attempt: int, kind: str, chunk: int | None = None) -> StreamObservation:
    text = "" if chunk is None else "ABC"[chunk]
    return StreamObservation(
        offset=offset,
        event=StreamEvent(
            logical_output_id="workflow-post-flush-duplicate/output",
            kind=kind,
            attempt=attempt,
            worker_id=f"worker-{attempt}",
            chunk_index=chunk,
            text=text,
        ),
    )


def _record(scenario: Scenario) -> TrialRecord:
    if scenario is Scenario.UNFAULTED:
        observations = [_event(index, 1, "chunk", index) for index in range(3)]
        observations.append(_event(3, 1, "complete"))
        batches = [PublishedBatch("publisher-1", 1, 1, (0, 1, 2, 3))]
    elif scenario is Scenario.PRE_FLUSH_LOSS:
        observations = [_event(0, 2, "retry")]
        observations.extend(_event(index + 1, 2, "chunk", index) for index in range(3))
        observations.append(_event(4, 2, "complete"))
        batches = [PublishedBatch("publisher-2", 1, 2, (0, 1, 2, 3, 4))]
    else:
        observations = [_event(index, 1, "chunk", index) for index in range(2)]
        observations.append(_event(2, 2, "retry"))
        observations.extend(_event(index + 3, 2, "chunk", index) for index in range(3))
        observations.append(_event(6, 2, "complete"))
        batches = [
            PublishedBatch("publisher-1", 1, 1, (0, 1)),
            PublishedBatch("publisher-2", 1, 2, (2, 3, 4, 5, 6)),
        ]
    workflow_id = f"workflow-{scenario.value}"
    observations = [
        replace(item, event=replace(item.event, logical_output_id=f"{workflow_id}/output"))
        for item in observations
    ]
    return TrialRecord(
        scenario=scenario,
        trial=1,
        workflow_id=workflow_id,
        run_id="run-1",
        expected_output="ABC",
        final_attempt=1 if scenario is Scenario.UNFAULTED else 2,
        final_worker_id="worker-1" if scenario is Scenario.UNFAULTED else "worker-2",
        acknowledged_offset=len(observations) - 1,
        observations=tuple(observations),
        batches=tuple(batches),
    )


@pytest.mark.parametrize("scenario", list(Scenario))
def test_oracle_distinguishes_buffer_loss_and_fresh_publisher_duplicates(
    scenario: Scenario,
) -> None:
    record = _record(scenario)
    verdict = audit_trial(record)
    assert verdict.retry_aware_output == "ABC"
    assert verdict.valid
    if scenario is Scenario.POST_FLUSH_DUPLICATE:
        assert verdict.naive_output == "ABABC"
        assert verdict.naive_duplicate_control_failed
    else:
        assert verdict.naive_output == "ABC"
        assert not verdict.naive_duplicate_control_failed


def test_oracle_rejects_offset_publisher_and_attempt_mutations() -> None:
    record = _record(Scenario.POST_FLUSH_DUPLICATE)
    mutations = [
        replace(record, observations=(record.observations[1], *record.observations[1:])),
        replace(record, batches=(record.batches[0], record.batches[0])),
        replace(record, final_attempt=1),
        replace(record, final_worker_id="wrong-worker"),
        replace(record, acknowledged_offset=0),
        replace(record, expected_output="wrong"),
        replace(record, final_attempt=True),
        replace(
            record,
            observations=(
                replace(record.observations[0], offset=False),
                replace(record.observations[1], offset=True),
                *record.observations[2:],
            ),
        ),
        replace(
            record,
            observations=(record.observations[0], *record.observations[2:]),
            batches=(
                replace(record.batches[0], offsets=(0,)),
                replace(record.batches[1], offsets=(1, 2, 3, 4, 5)),
            ),
            acknowledged_offset=5,
        ),
        replace(
            record,
            observations=(
                *record.observations[:-1],
                replace(
                    record.observations[-1],
                    event=replace(record.observations[-1].event, text="X", chunk_index=99),
                ),
            ),
        ),
        replace(
            record,
            observations=(
                replace(
                    record.observations[0],
                    event=replace(record.observations[0].event, logical_output_id="output-1"),
                ),
                *record.observations[1:],
            ),
        ),
    ]
    for mutated in mutations:
        with pytest.raises(ValueError):
            audit_trial(mutated)


def test_reconstructors_require_structural_events() -> None:
    observations = _record(Scenario.POST_FLUSH_DUPLICATE).observations
    assert reconstruct_naive(observations) == "ABABC"
    assert reconstruct_retry_aware(observations) == "ABC"
    with pytest.raises(ValueError):
        reconstruct_retry_aware(observations[:-1])
