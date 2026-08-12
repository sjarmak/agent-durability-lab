from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import activity
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from workflow_stream_retry.contract import (
    ActivityInput,
    ActivityResult,
    Scenario,
    StreamEvent,
    WorkflowInput,
)
from workflow_stream_retry.workflow import EVENTS_TOPIC, StreamRetryWorkflow


@activity.defn(name="publish_stream_output")
async def publish_without_fault(input: ActivityInput) -> ActivityResult:
    client = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(hours=1))
    topic = client.topic(EVENTS_TOPIC, type=StreamEvent)
    async with client:
        for index, text in enumerate(input.expected_output):
            topic.publish(
                StreamEvent(input.logical_output_id, "chunk", 1, "test-worker", index, text)
            )
        topic.publish(StreamEvent(input.logical_output_id, "complete", 1, "test-worker"))
    return ActivityResult(input.expected_output, 1, "test-worker")


async def test_workflow_holds_stream_open_until_exact_consumer_acknowledgement() -> None:
    environment = await WorkflowEnvironment.start_time_skipping()
    async with environment as temporal:
        task_queue = f"workflow-stream-unit-{uuid.uuid4()}"
        async with Worker(
            temporal.client,
            task_queue=task_queue,
            workflows=[StreamRetryWorkflow],
            activities=[publish_without_fault],
        ):
            workflow_id = f"workflow-stream-unit/{uuid.uuid4()}"
            handle = await temporal.client.start_workflow(
                StreamRetryWorkflow.run,
                WorkflowInput(Scenario.UNFAULTED, 1, "ABC"),
                id=workflow_id,
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            stream = WorkflowStreamClient.create(temporal.client, workflow_id)
            observations = []
            async for item in stream.subscribe(EVENTS_TOPIC, result_type=StreamEvent):
                observations.append(item)
                if item.data.kind == "complete":
                    await handle.signal(StreamRetryWorkflow.acknowledge, item.offset)
                    break
            result = await handle.result()
            history = await handle.fetch_history()

    assert [item.offset for item in observations] == [0, 1, 2, 3]
    assert result.full_text == "ABC"
    assert result.final_attempt == 1
    assert result.acknowledged_offset == 3
    await Replayer(workflows=[StreamRetryWorkflow]).replay_workflow(history)
