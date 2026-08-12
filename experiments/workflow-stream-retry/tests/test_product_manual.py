from __future__ import annotations

import base64
from dataclasses import replace
from types import SimpleNamespace

import pytest
from temporalio.contrib.workflow_streams import WorkflowStreamItem
from temporalio.converter import DataConverter

from workflow_stream_retry.product_contract import OutputEvent, OutputTerminal, WorkerChunk
from workflow_stream_retry.product_manual import (
    ManualLogicalOutputPublisher,
    ManualLogicalOutputReconstructor,
    validate_manual_acknowledgement,
)


def _publish(
    logical_output_id: str, generation: int, text: str
) -> tuple[list[OutputEvent], OutputTerminal]:
    events: list[OutputEvent] = []
    publisher = ManualLogicalOutputPublisher(
        topic="events",
        logical_output_id=logical_output_id,
        generation=generation,
        publisher_id=f"publisher-{generation}",
        activity_attempt=generation,
        worker_id=f"worker-{generation}",
        publish_event=events.append,
    )
    for chunk in text:
        publisher.publish(WorkerChunk(chunk, f"worker-{generation}"))
    return events, publisher.complete()


def test_manual_reference_resets_incremental_output_on_new_generation() -> None:
    first, _ = _publish("output", 1, "AB")
    replacement, terminal = _publish("output", 2, "ABC")
    reconstructor = ManualLogicalOutputReconstructor("output")

    updates = [
        reconstructor.apply(WorkflowStreamItem("events", event, offset))
        for offset, event in enumerate([*first[:-1], *replacement])
    ]

    assert [update.kind for update in updates] == [
        "begin",
        "chunk",
        "chunk",
        "begin",
        "chunk",
        "chunk",
        "chunk",
        "complete",
    ]
    assert updates[-1].result is not None
    assert updates[-1].result.items == ("A", "B", "C")
    assert updates[-1].result.terminal == terminal


def test_manual_reference_acknowledgement_is_bound_to_exact_log_item() -> None:
    events, terminal = _publish("output", 1, "ABC")
    reconstructor = ManualLogicalOutputReconstructor("output")
    result = None
    for offset, event in enumerate(events, start=7):
        result = reconstructor.apply(WorkflowStreamItem("events", event, offset)).result
    assert result is not None

    converter = DataConverter.default.payload_converter
    wire_log = []
    for event in events:
        payload = converter.to_payloads([event])[0]
        wire_log.append(
            SimpleNamespace(
                topic="events",
                data=base64.b64encode(payload.SerializeToString()).decode("ascii"),
            )
        )
    state = SimpleNamespace(base_offset=7, log=wire_log)
    validate_manual_acknowledgement(state, terminal, result.acknowledgement)

    with pytest.raises(ValueError, match="acknowledgement"):
        validate_manual_acknowledgement(
            state,
            terminal,
            replace(result.acknowledgement, terminal_offset=9),
        )
