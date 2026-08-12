from __future__ import annotations

import base64
from dataclasses import dataclass, replace

from temporalio.api.common.v1 import Payload
from temporalio.client import WorkflowHistory
from temporalio.contrib.workflow_streams import LogicalOutputEvent, PublishInput
from temporalio.converter import DataConverter, PayloadConverter

from .product_contract import Arm, OutputEvent, OutputObservation, WorkerChunk

PUBLISH_SIGNAL = "__temporal_workflow_stream_publish"


@dataclass(frozen=True)
class ProductBatchActor:
    activity_attempt: int
    identity: str


def inspect_product_stream(
    history: WorkflowHistory, arm: Arm
) -> tuple[tuple[OutputObservation, ...], int]:
    """Independently decode all durable stream publications in a history."""
    converter = DataConverter.default.payload_converter
    observations: list[OutputObservation] = []
    publishers: dict[str, int] = {}
    batch_count = 0
    for history_event in history.events:
        if not history_event.HasField("workflow_execution_signaled_event_attributes"):
            continue
        attributes = history_event.workflow_execution_signaled_event_attributes
        if attributes.signal_name != PUBLISH_SIGNAL:
            continue
        decoded = converter.from_payloads(attributes.input.payloads, [PublishInput])
        if len(decoded) != 1 or not isinstance(decoded[0], PublishInput):
            raise ValueError("stream publication payload differs")
        publication = decoded[0]
        if not publication.publisher_id or publication.sequence <= publishers.get(
            publication.publisher_id, 0
        ):
            raise ValueError("stream publication identity or sequence differs")
        publishers[publication.publisher_id] = publication.sequence
        batch_count += 1
        for entry in publication.items:
            payload = Payload.FromString(base64.b64decode(entry.data, validate=True))
            if arm is Arm.PRODUCT:
                event = converter.from_payload(payload, LogicalOutputEvent)
                normalized = _product_event(event, converter)
            else:
                normalized = converter.from_payload(payload, OutputEvent)
            observations.append(OutputObservation(len(observations), normalized))
    if arm is Arm.PRODUCT:
        observations = _fill_product_workers(observations)
    return tuple(observations), batch_count


def inspect_product_batch_actors(
    history: WorkflowHistory, arm: Arm
) -> tuple[ProductBatchActor, ...]:
    converter = DataConverter.default.payload_converter
    actors = []
    for history_event in history.events:
        if not history_event.HasField("workflow_execution_signaled_event_attributes"):
            continue
        attributes = history_event.workflow_execution_signaled_event_attributes
        if attributes.signal_name != PUBLISH_SIGNAL:
            continue
        decoded = converter.from_payloads(attributes.input.payloads, [PublishInput])
        if len(decoded) != 1 or not isinstance(decoded[0], PublishInput):
            raise ValueError("stream publication payload differs")
        attempts = set()
        for entry in decoded[0].items:
            payload = Payload.FromString(base64.b64decode(entry.data, validate=True))
            if arm is Arm.PRODUCT:
                attempt = converter.from_payload(payload, LogicalOutputEvent).activity_attempt
            else:
                attempt = converter.from_payload(payload, OutputEvent).activity_attempt
            attempts.add(attempt)
        if len(attempts) != 1 or not attributes.identity:
            raise ValueError("stream publication actor differs")
        attempt = attempts.pop()
        if type(attempt) is not int or attempt < 1:
            raise ValueError("stream publication Activity attempt differs")
        actors.append(ProductBatchActor(attempt, attributes.identity))
    return tuple(actors)


def _product_event(event: LogicalOutputEvent, converter: PayloadConverter) -> OutputEvent:
    chunk: WorkerChunk | None = None
    if event.data:
        payload = Payload.FromString(base64.b64decode(event.data, validate=True))
        chunk = converter.from_payload(payload, WorkerChunk)
    return OutputEvent(
        logical_output_id=event.logical_output_id,
        generation=event.generation,
        publisher_id=event.publisher_id,
        activity_attempt=event.activity_attempt or 0,
        worker_id="" if chunk is None else chunk.worker_id,
        kind=event.kind.value,
        sequence=event.sequence,
        chunk_index=event.chunk_index,
        text="" if chunk is None else chunk.text,
        chunk_count=event.chunk_count,
        terminal_sha256=event.content_sha256,
    )


def _fill_product_workers(
    observations: list[OutputObservation],
) -> list[OutputObservation]:
    workers = {
        item.event.publisher_id: item.event.worker_id
        for item in observations
        if item.event.worker_id
    }
    if any(not workers.get(item.event.publisher_id) for item in observations):
        raise ValueError("product publisher lacks a durable Worker identity")
    return [
        replace(item, event=replace(item.event, worker_id=workers[item.event.publisher_id]))
        for item in observations
    ]
