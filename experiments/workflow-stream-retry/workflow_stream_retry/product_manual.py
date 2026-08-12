from __future__ import annotations

import base64
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.contrib.workflow_streams import WorkflowStreamItem
from temporalio.converter import DataConverter, PayloadConverter

from .product_contract import (
    OutputAcknowledgement,
    OutputEvent,
    OutputTerminal,
    WorkerChunk,
)


@dataclass(frozen=True)
class ManualLogicalOutputResult:
    items: tuple[str, ...]
    terminal: OutputTerminal
    terminal_offset: int

    @property
    def acknowledgement(self) -> OutputAcknowledgement:
        return OutputAcknowledgement(
            topic=self.terminal.topic,
            logical_output_id=self.terminal.logical_output_id,
            generation=self.terminal.generation,
            terminal_sequence=self.terminal.terminal_sequence,
            terminal_offset=self.terminal_offset,
            chunk_count=self.terminal.chunk_count,
            content_sha256=self.terminal.content_sha256,
            publisher_id=self.terminal.publisher_id,
        )


@dataclass(frozen=True)
class ManualLogicalOutputUpdate:
    kind: str
    generation: int
    chunk: str | None = None
    result: ManualLogicalOutputResult | None = None


class ManualLogicalOutputPublisher:
    """Application-authored reference protocol for the product comparison."""

    def __init__(
        self,
        *,
        topic: str,
        logical_output_id: str,
        generation: int,
        publisher_id: str,
        activity_attempt: int,
        worker_id: str,
        publish_event: Callable[[OutputEvent], None],
        payload_converter: PayloadConverter | None = None,
    ) -> None:
        self._topic = topic
        self._logical_output_id = logical_output_id
        self._generation = generation
        self._publisher_id = publisher_id
        self._activity_attempt = activity_attempt
        self._worker_id = worker_id
        self._publish_event = publish_event
        self._payload_converter = payload_converter or DataConverter.default.payload_converter
        self._chunk_count = 0
        self._hasher = hashlib.sha256()
        self._complete = False
        self._publish_event(self._event("begin", sequence=0))

    def publish(self, value: WorkerChunk) -> None:
        if self._complete:
            raise ValueError("logical output generation is already complete")
        if value.worker_id != self._worker_id:
            raise ValueError("chunk Worker identity differs from its publisher")
        payload = self._payload_converter.to_payloads([value])[0]
        _update_hash(self._hasher, payload)
        index = self._chunk_count
        self._publish_event(
            self._event(
                "chunk",
                sequence=index + 1,
                chunk_index=index,
                text=value.text,
            )
        )
        self._chunk_count += 1

    def complete(self) -> OutputTerminal:
        if self._complete:
            raise ValueError("logical output generation is already complete")
        sequence = self._chunk_count + 1
        content_sha256 = self._hasher.hexdigest()
        self._publish_event(
            self._event(
                "complete",
                sequence=sequence,
                chunk_count=self._chunk_count,
                terminal_sha256=content_sha256,
            )
        )
        self._complete = True
        return OutputTerminal(
            topic=self._topic,
            logical_output_id=self._logical_output_id,
            generation=self._generation,
            terminal_sequence=sequence,
            chunk_count=self._chunk_count,
            content_sha256=content_sha256,
            publisher_id=self._publisher_id,
        )

    def _event(
        self,
        kind: str,
        *,
        sequence: int,
        chunk_index: int | None = None,
        text: str = "",
        chunk_count: int | None = None,
        terminal_sha256: str = "",
    ) -> OutputEvent:
        return OutputEvent(
            logical_output_id=self._logical_output_id,
            generation=self._generation,
            publisher_id=self._publisher_id,
            activity_attempt=self._activity_attempt,
            worker_id=self._worker_id,
            kind=kind,
            sequence=sequence,
            chunk_index=chunk_index,
            text=text,
            chunk_count=chunk_count,
            terminal_sha256=terminal_sha256,
        )


class ManualLogicalOutputReconstructor:
    """Expert application implementation used as the protected reference arm."""

    def __init__(
        self,
        logical_output_id: str,
        *,
        payload_converter: PayloadConverter | None = None,
    ) -> None:
        self._logical_output_id = logical_output_id
        self._payload_converter = payload_converter or DataConverter.default.payload_converter
        self._generation: int | None = None
        self._publisher_id = ""
        self._activity_attempt = 0
        self._worker_id = ""
        self._topic = ""
        self._items: list[str] = []
        self._hasher = hashlib.sha256()
        self._complete = False

    def apply(self, item: WorkflowStreamItem[OutputEvent]) -> ManualLogicalOutputUpdate:
        event = item.data
        if event.logical_output_id != self._logical_output_id:
            raise ValueError("logical output ID differs")
        if event.kind == "begin":
            self._begin(item.topic, event)
            return ManualLogicalOutputUpdate("begin", event.generation)
        self._validate_current(item.topic, event)
        if event.kind == "chunk":
            self._chunk(event)
            return ManualLogicalOutputUpdate("chunk", event.generation, chunk=event.text)
        if event.kind == "complete":
            result = self._terminal(item.offset, event)
            return ManualLogicalOutputUpdate("complete", event.generation, result=result)
        raise ValueError("logical output event kind differs")

    def _begin(self, topic: str, event: OutputEvent) -> None:
        if (
            event.sequence != 0
            or event.chunk_index is not None
            or event.text
            or event.chunk_count is not None
            or event.terminal_sha256
        ):
            raise ValueError("begin event fields differ")
        if self._generation is not None and event.generation <= self._generation:
            raise ValueError("generation is stale or repeated")
        self._generation = event.generation
        self._publisher_id = event.publisher_id
        self._activity_attempt = event.activity_attempt
        self._worker_id = event.worker_id
        self._topic = topic
        self._items = []
        self._hasher = hashlib.sha256()
        self._complete = False

    def _validate_current(self, topic: str, event: OutputEvent) -> None:
        if self._generation is None:
            raise ValueError("logical output generation did not begin")
        if (
            event.generation != self._generation
            or event.publisher_id != self._publisher_id
            or event.activity_attempt != self._activity_attempt
            or event.worker_id != self._worker_id
            or topic != self._topic
        ):
            raise ValueError("logical output generation authority differs")
        if self._complete:
            raise ValueError("logical output generation is already complete")
        if event.sequence != len(self._items) + 1:
            raise ValueError("logical output sequence differs")

    def _chunk(self, event: OutputEvent) -> None:
        if (
            event.chunk_index != len(self._items)
            or not event.text
            or event.chunk_count is not None
            or event.terminal_sha256
        ):
            raise ValueError("chunk fields differ")
        value = WorkerChunk(event.text, event.worker_id)
        payload = self._payload_converter.to_payloads([value])[0]
        _update_hash(self._hasher, payload)
        self._items.append(event.text)

    def _terminal(self, terminal_offset: int, event: OutputEvent) -> ManualLogicalOutputResult:
        if (
            event.chunk_index is not None
            or event.text
            or event.chunk_count != len(self._items)
            or event.terminal_sha256 != self._hasher.hexdigest()
        ):
            raise ValueError("terminal fields differ")
        self._complete = True
        terminal = OutputTerminal(
            topic=self._topic,
            logical_output_id=event.logical_output_id,
            generation=event.generation,
            terminal_sequence=event.sequence,
            chunk_count=len(self._items),
            content_sha256=event.terminal_sha256,
            publisher_id=event.publisher_id,
        )
        return ManualLogicalOutputResult(tuple(self._items), terminal, terminal_offset)


def validate_manual_acknowledgement(
    state: Any,
    terminal: OutputTerminal,
    acknowledgement: OutputAcknowledgement,
    *,
    payload_converter: PayloadConverter | None = None,
) -> None:
    """Application reference for exact terminal lookup in Workflow state."""
    expected = (
        terminal.topic,
        terminal.logical_output_id,
        terminal.generation,
        terminal.terminal_sequence,
        terminal.chunk_count,
        terminal.content_sha256,
        terminal.publisher_id,
    )
    actual = (
        acknowledgement.topic,
        acknowledgement.logical_output_id,
        acknowledgement.generation,
        acknowledgement.terminal_sequence,
        acknowledgement.chunk_count,
        acknowledgement.content_sha256,
        acknowledgement.publisher_id,
    )
    index = acknowledgement.terminal_offset - state.base_offset
    if actual != expected or index < 0 or index >= len(state.log):
        raise ValueError("acknowledgement does not match the successful terminal")
    wire_item = state.log[index]
    try:
        payload = Payload.FromString(base64.b64decode(wire_item.data, validate=True))
        converter = payload_converter or DataConverter.default.payload_converter
        event = converter.from_payload(payload, OutputEvent)
    except (TypeError, ValueError) as error:
        raise ValueError("acknowledgement log item cannot be decoded") from error
    observed = (
        wire_item.topic,
        event.logical_output_id,
        event.generation,
        event.sequence,
        event.chunk_count,
        event.terminal_sha256,
        event.publisher_id,
    )
    if (
        event.kind != "complete"
        or event.chunk_index is not None
        or event.text
        or observed != expected
    ):
        raise ValueError("acknowledgement does not match the exact log terminal")


def _update_hash(hasher: Any, payload: Payload) -> None:
    encoded = payload.SerializeToString()
    hasher.update(len(encoded).to_bytes(8, "big"))
    hasher.update(encoded)
