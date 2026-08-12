from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity
from temporalio.contrib.workflow_streams import TopicHandle, WorkflowStreamClient

from .barrier import BarrierArrival, BarrierClient
from .contract import ActivityInput, ActivityResult, Scenario, StreamEvent
from .workflow import ACTIVITY_NAME, EVENTS_TOPIC


@dataclass(frozen=True)
class ActivityRuntime:
    worker_id: str
    barrier: BarrierClient | None = None


class PublisherActivities:
    def __init__(self, runtime: ActivityRuntime) -> None:
        if not runtime.worker_id:
            raise ValueError("worker identity is required")
        self._runtime = runtime

    @activity.defn(name=ACTIVITY_NAME)
    async def publish(self, input: ActivityInput) -> ActivityResult:
        info = activity.info()
        attempt = info.attempt
        activity.heartbeat({"attempt": attempt, "worker_id": self._runtime.worker_id})
        client = WorkflowStreamClient.from_within_activity(batch_interval=timedelta(hours=1))
        topic = client.topic(EVENTS_TOPIC, type=StreamEvent)
        async with client:
            if attempt > 1:
                topic.publish(self._event(input, "retry", attempt))
            if attempt == 1 and input.scenario is not Scenario.UNFAULTED:
                self._publish_chunks(topic, input, attempt, limit=2)
                if input.scenario is Scenario.POST_FLUSH_DUPLICATE:
                    await client.flush()
                await self._arrive(input, attempt)
                raise RuntimeError("fault barrier returned without Worker loss")
            self._publish_chunks(topic, input, attempt, limit=len(input.expected_output))
            topic.publish(self._event(input, "complete", attempt))
            await client.flush()
        return ActivityResult(input.expected_output, attempt, self._runtime.worker_id)

    def _publish_chunks(
        self,
        topic: TopicHandle[StreamEvent],
        input: ActivityInput,
        attempt: int,
        *,
        limit: int,
    ) -> None:
        for index, text in enumerate(input.expected_output[:limit]):
            topic.publish(self._event(input, "chunk", attempt, chunk_index=index, text=text))

    async def _arrive(self, input: ActivityInput, attempt: int) -> None:
        if self._runtime.barrier is None:
            raise RuntimeError("fault scenario lacks a barrier")
        workflow_id = activity.info().workflow_id
        if workflow_id is None:
            raise RuntimeError("Activity lacks a Workflow identity")
        await self._runtime.barrier.arrive(
            BarrierArrival(
                point=input.scenario.value,
                workflow_id=workflow_id,
                attempt=attempt,
                worker_id=self._runtime.worker_id,
            )
        )

    def _event(
        self,
        input: ActivityInput,
        kind: str,
        attempt: int,
        *,
        chunk_index: int | None = None,
        text: str = "",
    ) -> StreamEvent:
        return StreamEvent(
            logical_output_id=input.logical_output_id,
            kind=kind,
            attempt=attempt,
            worker_id=self._runtime.worker_id,
            chunk_index=chunk_index,
            text=text,
        )
