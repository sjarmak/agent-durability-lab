from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from temporalio.client import (
    Client,
    WorkflowHandle,
    WorkflowHistory,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.workflow_streams import (
    LogicalOutputAcknowledgement,
    LogicalOutputEvent,
    LogicalOutputReconstructor,
    WorkflowStreamClient,
    WorkflowStreamItem,
)
from temporalio.worker import Replayer

from .barrier import BarrierArrival, BarrierServer
from .product_contract import (
    Arm,
    OutputAcknowledgement,
    OutputEvent,
    OutputObservation,
    ProductScenario,
    ProductTrialRecord,
    ProductTrialVerdict,
    ProductWorkflowInput,
    ProductWorkflowResult,
    WorkerChunk,
)
from .product_history import inspect_product_stream
from .product_manual import ManualLogicalOutputReconstructor
from .product_oracle import audit_product_trial
from .product_workflow import EVENTS_TOPIC, ProductStreamWorkflow
from .worker_process import WorkerProcess, launch_worker, stop_worker


@dataclass(frozen=True)
class ProductTrialCapture:
    record: ProductTrialRecord
    verdict: ProductTrialVerdict
    history: WorkflowHistory
    worker_processes: tuple[tuple[str, int, int], ...]
    fault_arrival: BarrierArrival | None
    started_at: str
    fault_at: str | None
    replacement_at: str | None
    completed_at: str


@dataclass(frozen=True)
class _Collection:
    observations: tuple[OutputObservation, ...]
    reconstructed_output: str
    stale_ack_rejections: int


async def run_product_trial(
    *,
    client: Client,
    project_root: Path,
    barrier: BarrierServer,
    arm: Arm,
    scenario: ProductScenario,
    trial: int,
    run_label: str,
) -> ProductTrialCapture:
    workflow_id = f"{run_label}-{arm.value}-{scenario.value}-trial-{trial}"
    task_queue = f"workflow-stream-product-{uuid.uuid4()}"
    first_id = f"{workflow_id}/worker-1"
    first = await launch_worker(
        project_root=project_root,
        address=client.service_client.config.target_host,
        task_queue=task_queue,
        worker_id=first_id,
        barrier=barrier,
        module="workflow_stream_retry.product_worker",
    )
    first_process_identity = f"{first.worker_id}/pid-{first.pid}"
    expected_arrival = BarrierArrival(scenario.value, workflow_id, 1, first_process_identity)
    if scenario is not ProductScenario.UNFAULTED:
        barrier.expect(expected_arrival)
    workers: list[WorkerProcess] = [first]
    fault_committed = asyncio.Event()
    started_at = _now()
    handle = await client.start_workflow(
        ProductStreamWorkflow.run,
        ProductWorkflowInput(arm, scenario, trial, "ABC"),
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=timedelta(seconds=45),
    )
    collection_task = asyncio.create_task(
        _collect(client, handle, workflow_id, arm, scenario, fault_committed)
    )
    fault_arrival: BarrierArrival | None = None
    fault_at: str | None = None
    replacement_at: str | None = None
    try:
        if scenario is not ProductScenario.UNFAULTED:
            fault_arrival = await asyncio.wait_for(barrier.next_arrival(), timeout=15)
            if fault_arrival != expected_arrival:
                raise RuntimeError("fault barrier identity differs")
            fault_at = _now()
            await stop_worker(first, kill=True)
            fault_committed.set()
            replacement = await launch_worker(
                project_root=project_root,
                address=client.service_client.config.target_host,
                task_queue=task_queue,
                worker_id=f"{workflow_id}/worker-2",
                barrier=None,
                module="workflow_stream_retry.product_worker",
            )
            workers.append(replacement)
            replacement_at = _now()
        result = await asyncio.wait_for(handle.result(), timeout=40)
        collection = await asyncio.wait_for(collection_task, timeout=5)
        history = await handle.fetch_history()
        durable_observations, stream_batch_count = inspect_product_stream(history, arm)
        if durable_observations != collection.observations:
            raise ValueError("consumer observations differ from durable stream history")
        completed_at = _now()
    finally:
        fault_committed.set()
        for worker in reversed(workers):
            await stop_worker(worker, kill=False)
        if not collection_task.done():
            collection_task.cancel()
    if not isinstance(result, ProductWorkflowResult):
        raise TypeError("Workflow result type differs")
    record = ProductTrialRecord(
        arm=arm,
        scenario=scenario,
        trial=trial,
        workflow_id=workflow_id,
        run_id=history.run_id,
        expected_output="ABC",
        final_attempt=result.final_attempt,
        final_worker_id=result.final_worker_id,
        final_terminal=result.terminal,
        acknowledgement=result.acknowledgement,
        stale_ack_rejections=collection.stale_ack_rejections,
        observations=collection.observations,
        stream_batch_count=stream_batch_count,
        history_event_count=len(history.events),
        history_json_bytes=len(history.to_json().encode("utf-8")),
    )
    verdict = audit_product_trial(record)
    if verdict.reconstructed_output != collection.reconstructed_output:
        raise ValueError("live reconstruction differs from the independent oracle")
    await Replayer(workflows=[ProductStreamWorkflow]).replay_workflow(history)
    return ProductTrialCapture(
        record,
        verdict,
        history,
        tuple((worker.worker_id, worker.pid, _exit_code(worker)) for worker in workers),
        fault_arrival,
        started_at,
        fault_at,
        replacement_at,
        completed_at,
    )


async def _collect(
    client: Client,
    handle: WorkflowHandle[ProductStreamWorkflow, ProductWorkflowResult],
    workflow_id: str,
    arm: Arm,
    scenario: ProductScenario,
    fault_committed: asyncio.Event,
) -> _Collection:
    stream = WorkflowStreamClient.create(client, workflow_id)
    logical_output_id = f"{workflow_id}/output"
    manual = ManualLogicalOutputReconstructor(logical_output_id) if arm is Arm.MANUAL else None
    product: LogicalOutputReconstructor[WorkerChunk] | None = None
    observations: list[OutputObservation] = []
    rendered: list[str] = []
    stale_ack_rejections = 0
    acknowledged = False
    final_generation = 1 if scenario is ProductScenario.UNFAULTED else 2
    result_type = LogicalOutputEvent if arm is Arm.PRODUCT else OutputEvent
    async for item in stream.subscribe(EVENTS_TOPIC, result_type=result_type):
        acknowledgement: OutputAcknowledgement | None = None
        if arm is Arm.PRODUCT:
            product_item = _product_item(item)
            if product is None:
                product = stream.logical_output_reconstructor(
                    logical_output_id, result_type=WorkerChunk
                )
            update = product.apply(product_item)
            event = _normalize_product_event(product_item.data, update.chunk)
            event = _fill_worker_identity(observations, event)
            if update.result is not None:
                acknowledgement = _product_ack(update.result.acknowledgement)
        else:
            event = _output_item(item).data
            if manual is not None:
                manual_update = manual.apply(_output_item(item))
                if manual_update.result is not None:
                    acknowledgement = manual_update.result.acknowledgement
        observations.append(OutputObservation(item.offset, event))
        if event.kind == "begin":
            rendered.clear()
        elif event.kind == "chunk":
            rendered.append(event.text)
        if event.kind != "complete":
            continue
        if arm is Arm.RAW:
            acknowledgement = _raw_ack(EVENTS_TOPIC, item.offset, event)
        if acknowledgement is None:
            raise RuntimeError("terminal lacks an acknowledgement receipt")
        if scenario is ProductScenario.TERMINAL_BEFORE_ACK and event.generation == 1:
            await fault_committed.wait()
            try:
                await handle.execute_update(ProductStreamWorkflow.acknowledge, acknowledgement)
            except WorkflowUpdateFailedError:
                if arm is Arm.RAW:
                    raise
                stale_ack_rejections += 1
            else:
                if arm is not Arm.RAW:
                    raise RuntimeError("protected arm accepted a stale terminal")
                acknowledged = True
            continue
        if event.generation != final_generation:
            continue
        if not acknowledged:
            await handle.execute_update(ProductStreamWorkflow.acknowledge, acknowledgement)
        await handle.signal(ProductStreamWorkflow.finish_collection)
        return _Collection(tuple(observations), "".join(rendered), stale_ack_rejections)
    raise RuntimeError("stream closed before the final generation terminal")


def _product_item(item: WorkflowStreamItem[object]) -> WorkflowStreamItem[LogicalOutputEvent]:
    if not isinstance(item.data, LogicalOutputEvent):
        raise TypeError("product stream item type differs")
    return WorkflowStreamItem(item.topic, item.data, item.offset)


def _output_item(item: WorkflowStreamItem[object]) -> WorkflowStreamItem[OutputEvent]:
    if not isinstance(item.data, OutputEvent):
        raise TypeError("reference stream item type differs")
    return WorkflowStreamItem(item.topic, item.data, item.offset)


def _normalize_product_event(event: LogicalOutputEvent, chunk: WorkerChunk | None) -> OutputEvent:
    worker_id = "" if chunk is None else chunk.worker_id
    return OutputEvent(
        logical_output_id=event.logical_output_id,
        generation=event.generation,
        publisher_id=event.publisher_id,
        activity_attempt=event.activity_attempt or 0,
        worker_id=worker_id,
        kind=event.kind.value,
        sequence=event.sequence,
        chunk_index=event.chunk_index,
        text="" if chunk is None else chunk.text,
        chunk_count=event.chunk_count,
        terminal_sha256=event.content_sha256,
    )


def _fill_worker_identity(observations: list[OutputObservation], event: OutputEvent) -> OutputEvent:
    if event.worker_id:
        for index, prior in enumerate(observations):
            if prior.event.publisher_id == event.publisher_id and not prior.event.worker_id:
                observations[index] = replace(
                    prior, event=replace(prior.event, worker_id=event.worker_id)
                )
        return event
    for prior in reversed(observations):
        if prior.event.publisher_id == event.publisher_id and prior.event.worker_id:
            return replace(event, worker_id=prior.event.worker_id)
    return event


def _product_ack(value: LogicalOutputAcknowledgement) -> OutputAcknowledgement:
    return OutputAcknowledgement(
        value.topic,
        value.logical_output_id,
        value.generation,
        value.terminal_sequence,
        value.terminal_offset,
        value.chunk_count,
        value.content_sha256,
        value.publisher_id,
    )


def _raw_ack(topic: str, offset: int, event: OutputEvent) -> OutputAcknowledgement:
    return OutputAcknowledgement(
        topic,
        event.logical_output_id,
        event.generation,
        event.sequence,
        offset,
        event.chunk_count or 0,
        event.terminal_sha256,
        event.publisher_id,
    )


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _exit_code(worker: WorkerProcess) -> int:
    if worker.process.returncode is None:
        raise RuntimeError("Worker process exit was not observed")
    return worker.process.returncode

