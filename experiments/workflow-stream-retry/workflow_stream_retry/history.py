from __future__ import annotations

import base64
from dataclasses import dataclass

from temporalio.api.common.v1 import Payload
from temporalio.client import WorkflowHistory
from temporalio.contrib.workflow_streams import PublishInput
from temporalio.converter import DataConverter

from .contract import (
    ActivityResult,
    PublishedBatch,
    StreamEvent,
    StreamObservation,
    WorkflowInput,
    WorkflowResult,
)

PUBLISH_SIGNAL = "__temporal_workflow_stream_publish"


@dataclass(frozen=True)
class BatchActor:
    activity_attempt: int
    identity: str


def inspect_stream_history(
    history: WorkflowHistory,
) -> tuple[tuple[PublishedBatch, ...], tuple[StreamObservation, ...]]:
    converter = DataConverter.default.payload_converter
    batches: list[PublishedBatch] = []
    observations: list[StreamObservation] = []
    next_offset = 0
    seen: dict[str, int] = {}
    for event in history.events:
        if not event.HasField("workflow_execution_signaled_event_attributes"):
            continue
        attributes = event.workflow_execution_signaled_event_attributes
        if attributes.signal_name != PUBLISH_SIGNAL:
            continue
        decoded = converter.from_payloads(attributes.input.payloads, [PublishInput])
        if len(decoded) != 1 or not isinstance(decoded[0], PublishInput):
            raise ValueError("stream publish signal payload is invalid")
        publish = decoded[0]
        prior_sequence = seen.get(publish.publisher_id, 0)
        if not publish.publisher_id or publish.sequence <= prior_sequence:
            raise ValueError("publisher identity or sequence is invalid")
        seen[publish.publisher_id] = publish.sequence
        attempts: set[int] = set()
        offsets: list[int] = []
        for entry in publish.items:
            payload = Payload.FromString(base64.b64decode(entry.data, validate=True))
            values = converter.from_payloads([payload], [StreamEvent])
            if len(values) != 1 or not isinstance(values[0], StreamEvent):
                raise ValueError("stream entry payload is invalid")
            attempts.add(values[0].attempt)
            observations.append(StreamObservation(next_offset, values[0]))
            offsets.append(next_offset)
            next_offset += 1
        if len(attempts) != 1:
            raise ValueError("publisher batch spans Activity attempts")
        batches.append(
            PublishedBatch(
                publisher_id=publish.publisher_id,
                sequence=publish.sequence,
                activity_attempt=attempts.pop(),
                offsets=tuple(offsets),
            )
        )
    return tuple(batches), tuple(observations)


def inspect_published_batches(history: WorkflowHistory) -> tuple[PublishedBatch, ...]:
    return inspect_stream_history(history)[0]


def batch_actors(history: WorkflowHistory) -> tuple[BatchActor, ...]:
    converter = DataConverter.default.payload_converter
    actors: list[BatchActor] = []
    for event in history.events:
        if not event.HasField("workflow_execution_signaled_event_attributes"):
            continue
        attributes = event.workflow_execution_signaled_event_attributes
        if attributes.signal_name != PUBLISH_SIGNAL:
            continue
        decoded = converter.from_payloads(attributes.input.payloads, [PublishInput])
        publish = decoded[0]
        attempts: set[int] = set()
        for entry in publish.items:
            payload = Payload.FromString(base64.b64decode(entry.data, validate=True))
            attempts.add(converter.from_payloads([payload], [StreamEvent])[0].attempt)
        if len(attempts) != 1 or not attributes.identity:
            raise ValueError("stream publish actor identity is invalid")
        actors.append(BatchActor(attempts.pop(), attributes.identity))
    return tuple(actors)


def activity_attempts(history: WorkflowHistory) -> tuple[tuple[int, str], ...]:
    attempts: list[tuple[int, str]] = []
    for event in history.events:
        if event.HasField("activity_task_started_event_attributes"):
            attributes = event.activity_task_started_event_attributes
            attempts.append((attributes.attempt, attributes.identity))
    return tuple(attempts)


def activity_retry_failures(history: WorkflowHistory) -> tuple[tuple[int, int | None], ...]:
    failures: list[tuple[int, int | None]] = []
    for event in history.events:
        if not event.HasField("activity_task_started_event_attributes"):
            continue
        attributes = event.activity_task_started_event_attributes
        timeout_type: int | None = None
        if attributes.HasField("last_failure"):
            failure = attributes.last_failure
            if not failure.HasField("timeout_failure_info"):
                failures.append((attributes.attempt, -1))
                continue
            timeout_type = failure.timeout_failure_info.timeout_type
        failures.append((attributes.attempt, timeout_type))
    return tuple(failures)


def workflow_result(history: WorkflowHistory) -> WorkflowResult:
    converter = DataConverter.default.payload_converter
    results: list[WorkflowResult] = []
    for event in history.events:
        if not event.HasField("workflow_execution_completed_event_attributes"):
            continue
        attributes = event.workflow_execution_completed_event_attributes
        decoded = converter.from_payloads(attributes.result.payloads, [WorkflowResult])
        if len(decoded) != 1 or not isinstance(decoded[0], WorkflowResult):
            raise ValueError("Workflow result payload is invalid")
        results.append(decoded[0])
    if len(results) != 1:
        raise ValueError("Workflow completion result count differs")
    return results[0]


def workflow_input(history: WorkflowHistory) -> WorkflowInput:
    converter = DataConverter.default.payload_converter
    inputs: list[WorkflowInput] = []
    for event in history.events:
        if not event.HasField("workflow_execution_started_event_attributes"):
            continue
        attributes = event.workflow_execution_started_event_attributes
        decoded = converter.from_payloads(attributes.input.payloads, [WorkflowInput])
        if len(decoded) != 1 or not isinstance(decoded[0], WorkflowInput):
            raise ValueError("Workflow input payload is invalid")
        inputs.append(decoded[0])
    if len(inputs) != 1:
        raise ValueError("Workflow input count differs")
    return inputs[0]


def activity_result(history: WorkflowHistory) -> ActivityResult:
    converter = DataConverter.default.payload_converter
    results: list[ActivityResult] = []
    for event in history.events:
        if not event.HasField("activity_task_completed_event_attributes"):
            continue
        attributes = event.activity_task_completed_event_attributes
        decoded = converter.from_payloads(attributes.result.payloads, [ActivityResult])
        if len(decoded) != 1 or not isinstance(decoded[0], ActivityResult):
            raise ValueError("Activity result payload is invalid")
        results.append(decoded[0])
    if len(results) != 1:
        raise ValueError("Activity completion result count differs")
    return results[0]
