from __future__ import annotations

from collections.abc import Sequence

from .contract import Scenario, StreamObservation, TrialRecord, TrialVerdict


def reconstruct_naive(observations: Sequence[StreamObservation]) -> str:
    _require_terminal(observations)
    return "".join(item.event.text for item in observations if item.event.kind == "chunk")


def reconstruct_retry_aware(observations: Sequence[StreamObservation]) -> str:
    _require_terminal(observations)
    chunks: list[str] = []
    expected_chunk = 0
    for observation in observations:
        event = observation.event
        if event.kind == "retry":
            if event.chunk_index is not None or event.text:
                raise ValueError("retry marker payload is invalid")
            chunks = []
            expected_chunk = 0
        elif event.kind == "chunk":
            if event.chunk_index != expected_chunk or len(event.text) != 1:
                raise ValueError("stream chunk sequence is invalid")
            chunks.append(event.text)
            expected_chunk += 1
        elif event.kind == "complete":
            if event.chunk_index is not None or event.text:
                raise ValueError("stream completion payload is invalid")
        else:
            raise ValueError("stream event kind is invalid")
    return "".join(chunks)


def audit_trial(record: TrialRecord) -> TrialVerdict:
    _validate_types(record)
    if (
        record.trial < 1
        or not record.workflow_id
        or not record.run_id
        or record.expected_output != "ABC"
    ):
        raise ValueError("trial identity is incomplete")
    if tuple(item.offset for item in record.observations) != tuple(range(len(record.observations))):
        raise ValueError("stream offsets are not contiguous")
    logical_ids = {item.event.logical_output_id for item in record.observations}
    if logical_ids != {f"{record.workflow_id}/output"}:
        raise ValueError("logical output identity differs")
    _validate_batches(record)
    _validate_layout(record)
    expected_attempt = 1 if record.scenario is Scenario.UNFAULTED else 2
    if record.final_attempt != expected_attempt:
        raise ValueError("final Activity attempt differs")
    if record.acknowledged_offset != record.observations[-1].offset:
        raise ValueError("consumer acknowledgement differs from terminal offset")
    retry_count = sum(item.event.kind == "retry" for item in record.observations)
    if retry_count != (0 if record.scenario is Scenario.UNFAULTED else 1):
        raise ValueError("retry marker count differs")
    attempts = {item.event.attempt for item in record.observations}
    expected_attempts = {
        Scenario.UNFAULTED: {1},
        Scenario.PRE_FLUSH_LOSS: {2},
        Scenario.POST_FLUSH_DUPLICATE: {1, 2},
    }[record.scenario]
    if attempts != expected_attempts:
        raise ValueError("durable publisher attempts differ")
    final_workers = {
        item.event.worker_id
        for item in record.observations
        if item.event.attempt == record.final_attempt
    }
    if final_workers != {record.final_worker_id}:
        raise ValueError("final Worker process identity differs")

    naive = reconstruct_naive(record.observations)
    retry_aware = reconstruct_retry_aware(record.observations)
    if retry_aware != record.expected_output:
        raise ValueError("retry-aware reconstruction differs")
    duplicate_control_failed = naive != record.expected_output
    if duplicate_control_failed != (record.scenario is Scenario.POST_FLUSH_DUPLICATE):
        raise ValueError("naive duplicate control did not distinguish the post-flush retry")
    return TrialVerdict(
        valid=True,
        naive_output=naive,
        retry_aware_output=retry_aware,
        naive_duplicate_control_failed=duplicate_control_failed,
    )


def _validate_batches(record: TrialRecord) -> None:
    expected_count = 2 if record.scenario is Scenario.POST_FLUSH_DUPLICATE else 1
    if len(record.batches) != expected_count:
        raise ValueError("publisher batch count differs")
    if len({batch.publisher_id for batch in record.batches}) != len(record.batches):
        raise ValueError("fresh Activity attempts reused a publisher identity")
    covered: list[int] = []
    for batch in record.batches:
        if not batch.publisher_id or batch.sequence != 1 or batch.activity_attempt < 1:
            raise ValueError("publisher batch identity is invalid")
        for offset in batch.offsets:
            if record.observations[offset].event.attempt != batch.activity_attempt:
                raise ValueError("publisher batch attempt differs from event")
        covered.extend(batch.offsets)
    if sorted(covered) != list(range(len(record.observations))):
        raise ValueError("publisher batches do not cover the stream exactly")


def _validate_layout(record: TrialRecord) -> None:
    expected = {
        Scenario.UNFAULTED: (
            (1, "chunk", 0, "A"),
            (1, "chunk", 1, "B"),
            (1, "chunk", 2, "C"),
            (1, "complete", None, ""),
        ),
        Scenario.PRE_FLUSH_LOSS: (
            (2, "retry", None, ""),
            (2, "chunk", 0, "A"),
            (2, "chunk", 1, "B"),
            (2, "chunk", 2, "C"),
            (2, "complete", None, ""),
        ),
        Scenario.POST_FLUSH_DUPLICATE: (
            (1, "chunk", 0, "A"),
            (1, "chunk", 1, "B"),
            (2, "retry", None, ""),
            (2, "chunk", 0, "A"),
            (2, "chunk", 1, "B"),
            (2, "chunk", 2, "C"),
            (2, "complete", None, ""),
        ),
    }[record.scenario]
    actual = tuple(
        (event.attempt, event.kind, event.chunk_index, event.text)
        for event in (observation.event for observation in record.observations)
    )
    if actual != expected:
        raise ValueError("stream event layout differs from the exact fault boundary")


def _require_terminal(observations: Sequence[StreamObservation]) -> None:
    if not observations or observations[-1].event.kind != "complete":
        raise ValueError("stream lacks a terminal completion event")
    if sum(item.event.kind == "complete" for item in observations) != 1:
        raise ValueError("stream completion count differs")


def _validate_types(record: TrialRecord) -> None:
    if (
        type(record.trial) is not int
        or type(record.workflow_id) is not str
        or type(record.run_id) is not str
        or type(record.expected_output) is not str
        or type(record.final_attempt) is not int
        or type(record.final_worker_id) is not str
        or type(record.acknowledged_offset) is not int
    ):
        raise ValueError("trial scalar type differs")
    for observation in record.observations:
        event = observation.event
        if (
            type(observation.offset) is not int
            or type(event.logical_output_id) is not str
            or type(event.kind) is not str
            or type(event.attempt) is not int
            or type(event.worker_id) is not str
            or (event.chunk_index is not None and type(event.chunk_index) is not int)
            or type(event.text) is not str
        ):
            raise ValueError("stream event scalar type differs")
    for batch in record.batches:
        if (
            type(batch.publisher_id) is not str
            or type(batch.sequence) is not int
            or type(batch.activity_attempt) is not int
            or any(type(offset) is not int for offset in batch.offsets)
        ):
            raise ValueError("publisher batch scalar type differs")
