from __future__ import annotations

from collections import defaultdict

from .product_contract import (
    Arm,
    OutputAcknowledgement,
    OutputEvent,
    OutputObservation,
    OutputTerminal,
    ProductScenario,
    ProductTrialRecord,
    ProductTrialVerdict,
)


def audit_product_trial(record: ProductTrialRecord) -> ProductTrialVerdict:
    _validate_identity(record)
    by_generation = _validate_events(record)
    expected_generations = {
        ProductScenario.UNFAULTED: {1},
        ProductScenario.PRE_FLUSH_LOSS: {2},
        ProductScenario.POST_FLUSH_PREFIX: {1, 2},
        ProductScenario.TERMINAL_BEFORE_ACK: {1, 2},
    }[record.scenario]
    if set(by_generation) != expected_generations:
        raise ValueError("observed generations differ from the frozen fault schedule")

    expected_attempt = 1 if record.scenario is ProductScenario.UNFAULTED else 2
    if record.final_attempt != expected_attempt:
        raise ValueError("final Activity attempt differs")
    final = by_generation[expected_attempt]
    final_chunks = "".join(item.event.text for item in final if item.event.kind == "chunk")
    final_event = final[-1].event
    if (
        final_chunks != record.expected_output
        or not final
        or final[-1].event.kind != "complete"
        or {item.event.worker_id for item in final} != {record.final_worker_id}
    ):
        raise ValueError("final generation differs from the Activity result")
    if _terminal_identity(record.final_terminal) != _event_terminal_identity(final_event):
        raise ValueError("successful Activity terminal differs from the final stream event")
    _validate_fault_layout(record, by_generation)
    acknowledged = next(
        (
            item
            for item in record.observations
            if item.offset == record.acknowledgement.terminal_offset
        ),
        None,
    )
    if (
        acknowledged is None
        or acknowledged.event.kind != "complete"
        or _acknowledgement_identity(record.acknowledgement)
        != _event_terminal_identity(acknowledged.event)
    ):
        raise ValueError("acknowledgement does not name an observed terminal")

    raw = "".join(item.event.text for item in record.observations if item.event.kind == "chunk")
    reconstructed = raw if record.arm is Arm.RAW else final_chunks
    duplicate = reconstructed != record.expected_output
    stale_ack = record.acknowledgement.generation != record.final_attempt
    if record.arm is Arm.RAW:
        expected_duplicate = record.scenario in {
            ProductScenario.POST_FLUSH_PREFIX,
            ProductScenario.TERMINAL_BEFORE_ACK,
        }
        expected_stale = record.scenario is ProductScenario.TERMINAL_BEFORE_ACK
        if (
            duplicate != expected_duplicate
            or stale_ack != expected_stale
            or record.stale_ack_rejections != 0
        ):
            raise ValueError("raw negative control did not distinguish the fault")
    elif (
        duplicate
        or stale_ack
        or record.stale_ack_rejections
        != (1 if record.scenario is ProductScenario.TERMINAL_BEFORE_ACK else 0)
    ):
        raise ValueError("protected arm did not fence retry output and acknowledgement")

    return ProductTrialVerdict(
        valid=True,
        raw_concatenation=raw,
        reconstructed_output=reconstructed,
        duplicate_output=duplicate,
        stale_ack_accepted=stale_ack,
        stale_ack_rejections=record.stale_ack_rejections,
    )


def _validate_identity(record: ProductTrialRecord) -> None:
    if (
        type(record.trial) is not int
        or record.trial < 1
        or not record.workflow_id
        or not record.run_id
        or record.expected_output != "ABC"
        or not record.observations
    ):
        raise ValueError("trial identity is incomplete")
    if tuple(item.offset for item in record.observations) != tuple(range(len(record.observations))):
        raise ValueError("stream offsets are not contiguous")
    if {item.event.logical_output_id for item in record.observations} != {
        f"{record.workflow_id}/output"
    }:
        raise ValueError("logical output identity differs")
    if (
        record.stream_batch_count < 1
        or record.history_event_count < 1
        or record.history_json_bytes < 1
    ):
        raise ValueError("history metrics are incomplete")


def _validate_events(
    record: ProductTrialRecord,
) -> dict[int, list[OutputObservation]]:
    grouped: dict[int, list[OutputObservation]] = defaultdict(list)
    for observation in record.observations:
        event = observation.event
        if (
            event.generation < 1
            or event.activity_attempt != event.generation
            or not event.publisher_id
            or not event.worker_id
        ):
            raise ValueError("event authority identity is invalid")
        grouped[event.generation].append(observation)

    for observations in grouped.values():
        if len({item.event.publisher_id for item in observations}) != 1:
            raise ValueError("one generation used competing publishers")
        structural = record.arm is not Arm.RAW
        position = 0
        if structural:
            begin = observations[0].event
            if (
                begin.kind != "begin"
                or begin.sequence != 0
                or begin.chunk_index is not None
                or begin.text
            ):
                raise ValueError("generation begin marker is invalid")
            position = 1
        elif observations[0].event.kind == "begin":
            raise ValueError("raw baseline unexpectedly contains a reset marker")

        chunk_index = 0
        terminal_seen = False
        for observation in observations[position:]:
            event = observation.event
            if terminal_seen:
                raise ValueError("event follows a terminal in one generation")
            if event.kind == "chunk":
                if (
                    event.sequence != chunk_index + 1
                    or event.chunk_index != chunk_index
                    or len(event.text) != 1
                    or event.chunk_count is not None
                    or event.terminal_sha256
                ):
                    raise ValueError("chunk sequence is invalid")
                chunk_index += 1
            elif event.kind == "complete":
                if (
                    event.sequence != chunk_index + 1
                    or event.chunk_index is not None
                    or event.text
                    or event.chunk_count != chunk_index
                    or len(event.terminal_sha256) != 64
                ):
                    raise ValueError("terminal sequence is invalid")
                terminal_seen = True
            else:
                raise ValueError("output event kind is invalid")
    return dict(grouped)


def _validate_fault_layout(
    record: ProductTrialRecord,
    grouped: dict[int, list[OutputObservation]],
) -> None:
    if record.scenario is ProductScenario.POST_FLUSH_PREFIX:
        first = grouped[1]
        first_chunks = "".join(item.event.text for item in first if item.event.kind == "chunk")
        if first_chunks != "AB" or any(item.event.kind == "complete" for item in first):
            raise ValueError("post-flush prefix boundary differs")
    elif record.scenario is ProductScenario.TERMINAL_BEFORE_ACK:
        first = grouped[1]
        if (
            "".join(item.event.text for item in first if item.event.kind == "chunk") != "ABC"
            or first[-1].event.kind != "complete"
        ):
            raise ValueError("terminal-before-ack boundary differs")


def _terminal_identity(terminal: OutputTerminal) -> tuple[object, ...]:
    return (
        terminal.logical_output_id,
        terminal.generation,
        terminal.terminal_sequence,
        terminal.chunk_count,
        terminal.content_sha256,
        terminal.publisher_id,
    )


def _acknowledgement_identity(
    acknowledgement: OutputAcknowledgement,
) -> tuple[object, ...]:
    return (
        acknowledgement.logical_output_id,
        acknowledgement.generation,
        acknowledgement.terminal_sequence,
        acknowledgement.chunk_count,
        acknowledgement.content_sha256,
        acknowledgement.publisher_id,
    )


def _event_terminal_identity(event: OutputEvent) -> tuple[object, ...]:
    return (
        event.logical_output_id,
        event.generation,
        event.sequence,
        event.chunk_count,
        event.terminal_sha256,
        event.publisher_id,
    )
