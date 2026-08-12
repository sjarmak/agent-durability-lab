from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from temporalio import activity
from temporalio.api.common.v1 import Payload
from temporalio.contrib.workflow_streams import (
    LogicalOutputTerminal,
    TopicHandle,
    WorkflowStreamClient,
)
from temporalio.converter import DataConverter

from .barrier import BarrierArrival, BarrierClient
from .product_contract import (
    Arm,
    OutputEvent,
    OutputTerminal,
    ProductActivityInput,
    ProductActivityResult,
    ProductScenario,
    WorkerChunk,
)
from .product_manual import ManualLogicalOutputPublisher
from .product_workflow import EVENTS_TOPIC, PRODUCT_ACTIVITY_NAME


@dataclass(frozen=True)
class ProductActivityRuntime:
    worker_id: str
    barrier: BarrierClient | None = None


class _Publisher(Protocol):
    def publish(self, value: WorkerChunk) -> None: ...

    def complete(self) -> OutputTerminal | LogicalOutputTerminal: ...


class _RawPublisher:
    def __init__(
        self,
        topic: TopicHandle[OutputEvent],
        *,
        topic_name: str,
        logical_output_id: str,
        generation: int,
        publisher_id: str,
        worker_id: str,
    ) -> None:
        self._topic = topic
        self._topic_name = topic_name
        self._logical_output_id = logical_output_id
        self._generation = generation
        self._publisher_id = publisher_id
        self._worker_id = worker_id
        self._count = 0
        self._hasher = hashlib.sha256()

    def publish(self, value: WorkerChunk) -> None:
        payload = DataConverter.default.payload_converter.to_payloads([value])[0]
        _update_hash(self._hasher, payload)
        index = self._count
        self._topic.publish(
            OutputEvent(
                logical_output_id=self._logical_output_id,
                generation=self._generation,
                publisher_id=self._publisher_id,
                activity_attempt=self._generation,
                worker_id=self._worker_id,
                kind="chunk",
                sequence=index + 1,
                chunk_index=index,
                text=value.text,
            )
        )
        self._count += 1

    def complete(self) -> OutputTerminal:
        sequence = self._count + 1
        digest = self._hasher.hexdigest()
        self._topic.publish(
            OutputEvent(
                logical_output_id=self._logical_output_id,
                generation=self._generation,
                publisher_id=self._publisher_id,
                activity_attempt=self._generation,
                worker_id=self._worker_id,
                kind="complete",
                sequence=sequence,
                chunk_count=self._count,
                terminal_sha256=digest,
            )
        )
        return OutputTerminal(
            self._topic_name,
            self._logical_output_id,
            self._generation,
            sequence,
            self._count,
            digest,
            self._publisher_id,
        )


class ProductPublisherActivities:
    def __init__(self, runtime: ProductActivityRuntime) -> None:
        self._runtime = runtime

    @activity.defn(name=PRODUCT_ACTIVITY_NAME)
    async def publish(self, input: ProductActivityInput) -> ProductActivityResult:
        info = activity.info()
        attempt = info.attempt
        activity.heartbeat({"attempt": attempt, "worker_id": self._runtime.worker_id})
        stream = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(hours=1))
        publisher = self._publisher(stream, input, attempt)
        terminal: OutputTerminal | LogicalOutputTerminal
        async with stream:
            faulted = attempt == 1 and input.scenario is not ProductScenario.UNFAULTED
            if faulted:
                limit = (
                    len(input.expected_output)
                    if input.scenario is ProductScenario.TERMINAL_BEFORE_ACK
                    else 2
                )
                self._publish_chunks(publisher, input.expected_output[:limit])
                if input.scenario is ProductScenario.TERMINAL_BEFORE_ACK:
                    terminal = publisher.complete()
                if input.scenario is not ProductScenario.PRE_FLUSH_LOSS:
                    await stream.flush()
                await self._arrive(input, attempt)
                raise RuntimeError("fault barrier returned without Worker loss")
            self._publish_chunks(publisher, input.expected_output)
            terminal = publisher.complete()
            await stream.flush()
        return ProductActivityResult(
            input.expected_output,
            attempt,
            self._runtime.worker_id,
            _terminal(terminal),
        )

    def _publisher(
        self,
        stream: WorkflowStreamClient,
        input: ProductActivityInput,
        attempt: int,
    ) -> _Publisher:
        if input.arm is Arm.PRODUCT:
            return stream.logical_output_publisher(
                EVENTS_TOPIC,
                logical_output_id=input.logical_output_id,
                generation=attempt,
                activity_attempt=attempt,
                type=WorkerChunk,
            )
        topic = stream.topic(EVENTS_TOPIC, type=OutputEvent)
        publisher_id = str(uuid.uuid4())
        if input.arm is Arm.MANUAL:
            return ManualLogicalOutputPublisher(
                topic=EVENTS_TOPIC,
                logical_output_id=input.logical_output_id,
                generation=attempt,
                publisher_id=publisher_id,
                activity_attempt=attempt,
                worker_id=self._runtime.worker_id,
                publish_event=topic.publish,
            )
        return _RawPublisher(
            topic,
            topic_name=EVENTS_TOPIC,
            logical_output_id=input.logical_output_id,
            generation=attempt,
            publisher_id=publisher_id,
            worker_id=self._runtime.worker_id,
        )

    def _publish_chunks(self, publisher: _Publisher, text: str) -> None:
        for chunk in text:
            publisher.publish(WorkerChunk(chunk, self._runtime.worker_id))

    async def _arrive(self, input: ProductActivityInput, attempt: int) -> None:
        if self._runtime.barrier is None:
            raise RuntimeError("fault scenario lacks a barrier")
        workflow_id = activity.info().workflow_id
        if workflow_id is None:
            raise RuntimeError("Activity lacks a Workflow identity")
        await self._runtime.barrier.arrive(
            BarrierArrival(
                input.scenario.value,
                workflow_id,
                attempt,
                self._runtime.worker_id,
            )
        )


def _terminal(value: OutputTerminal | LogicalOutputTerminal) -> OutputTerminal:
    if isinstance(value, OutputTerminal):
        return value
    return OutputTerminal(
        value.topic,
        value.logical_output_id,
        value.generation,
        value.terminal_sequence,
        value.chunk_count,
        value.content_sha256,
        value.publisher_id,
    )


def _update_hash(hasher: Any, payload: Payload) -> None:
    encoded = payload.SerializeToString()
    hasher.update(len(encoded).to_bytes(8, "big"))
    hasher.update(encoded)
