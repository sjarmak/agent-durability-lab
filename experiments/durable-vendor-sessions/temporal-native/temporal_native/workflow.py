"""Temporal Workflow that hosts the OpenAI Agents orchestration loop."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.workflow_streams import WorkflowStream, WorkflowStreamState

with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner
    from temporalio.contrib import openai_agents

    from temporal_native.activities import ToolActivities
    from temporal_native.contract import (
        AgentEvent,
        AgentTurnEnvelope,
        StrictModel,
        TurnIdentity,
        TurnInput,
        TurnResult,
    )

AGENT_EVENTS_TOPIC = "agent-events"
STREAM_DONE_TOPIC = "agent-stream-done"
STREAM_DRAIN_INTERVAL = timedelta(milliseconds=100)


class WorkflowStatus(StrictModel):
    """Query-visible durable lifecycle state."""

    phase: str
    approved: bool
    identity: TurnIdentity | None = None


@workflow.defn
class TemporalNativeAgentWorkflow:
    """One logical typed model/tool turn with Workflow-owned identity."""

    @workflow.init
    def __init__(
        self,
        turn_input: TurnInput,
        prior_identity: TurnIdentity | None = None,
        stream_state: WorkflowStreamState | None = None,
    ) -> None:
        TurnInput.model_validate(turn_input)
        self._phase = "created"
        self._approved = False
        self._result_released = False
        self._identity = (
            TurnIdentity.model_validate(prior_identity) if prior_identity is not None else None
        )
        self.stream = WorkflowStream(prior_state=stream_state)
        self.events = self.stream.topic(AGENT_EVENTS_TOPIC, type=AgentEvent)
        self.done = self.stream.topic(STREAM_DONE_TOPIC, type=bool)

    @workflow.run
    async def run(
        self,
        turn_input: TurnInput,
        prior_identity: TurnIdentity | None = None,
        stream_state: WorkflowStreamState | None = None,
    ) -> TurnResult:
        del stream_state
        turn_input = TurnInput.model_validate(turn_input)
        prior_identity = (
            TurnIdentity.model_validate(prior_identity) if prior_identity is not None else None
        )
        if prior_identity is not None and self._identity != prior_identity:
            raise ValueError("continuation identity differs from initialized identity")
        await self._initialize_session(turn_input)
        completed = False
        try:
            await self._await_approval(turn_input)
            result = await self._execute_agent(turn_input)
            await self._hold_result(turn_input, result)
            self._phase = "completed"
            completed = True
            return result
        finally:
            await self._cleanup(completed)

    async def _initialize_session(self, turn_input: TurnInput) -> None:
        if self._identity is not None:
            return
        self._identity = TurnIdentity.for_workflow(
            workflow.info().workflow_id,
            turn=1,
            owner_capability=str(workflow.uuid4()),
        )
        self._publish("session_started", workflow.info().run_id)
        if turn_input.continue_before_agent:
            self._phase = "continuing_as_new"
            await self.stream.continue_as_new(lambda state: [turn_input, self._identity, state])

    async def _await_approval(self, turn_input: TurnInput) -> None:
        if not turn_input.approval_required:
            self._approved = True
            return
        self._phase = "awaiting_approval"
        await workflow.wait_condition(lambda: self._approved)

    async def _execute_agent(self, turn_input: TurnInput) -> TurnResult:
        if self._identity is None:
            raise RuntimeError("agent cannot run before Workflow identity is minted")
        self._phase = "running_agent"
        self._publish("agent_started", workflow.info().run_id)
        envelope = AgentTurnEnvelope(
            **self._identity.model_dump(),
            task=turn_input.task,
            relative_path=turn_input.relative_path,
            content=turn_input.content,
        )
        run_result = Runner.run_streamed(
            starting_agent=self._agent(),
            input=envelope.model_dump_json(),
            max_turns=3,
        )
        async for event in run_result.stream_events():
            if event.type != "run_item_stream_event":
                continue
            if event.item.type == "tool_call_item":
                self._publish("tool_call", event.item.raw_item.name)
            elif event.item.type == "tool_call_output_item":
                self._publish("tool_output", str(event.item.output))
        result = run_result.final_output_as(TurnResult, raise_if_incorrect_type=True)
        result.require_identity(self._identity)
        return result

    @staticmethod
    def _agent() -> Agent[TurnResult]:
        return Agent(
            name="Temporal native fixture agent",
            instructions=(
                "Apply exactly the supplied fixture edit with the available tool, "
                "then return the correlated structured receipt."
            ),
            tools=[
                openai_agents.workflow.activity_as_tool(
                    ToolActivities.apply_fixture_change,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            ],
            output_type=TurnResult,
        )

    async def _hold_result(self, turn_input: TurnInput, result: TurnResult) -> None:
        self._phase = "result_built"
        self._publish("result_built", result.destination_receipt)
        if turn_input.hold_result:
            await workflow.wait_condition(lambda: self._result_released)

    async def _cleanup(self, completed: bool) -> None:
        if self._identity is None:
            raise RuntimeError("cleanup cannot run before Workflow identity is minted")
        self._phase = "cleaning_up"
        await workflow.execute_activity(
            ToolActivities.record_cleanup,
            args=[self._identity, workflow.cancellation_reason() or "workflow completed"],
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        if completed:
            self.done.publish(True)
            await workflow.sleep(STREAM_DRAIN_INTERVAL)

    @workflow.signal
    def approve(self) -> None:
        self._approved = True

    @workflow.signal
    def release_result(self) -> None:
        self._result_released = True

    @workflow.query
    def status(self) -> WorkflowStatus:
        return WorkflowStatus(
            phase=self._phase,
            approved=self._approved,
            identity=self._identity,
        )

    def _publish(self, kind: str, detail: str) -> None:
        if self._identity is None:
            raise RuntimeError("cannot publish before Workflow identity is minted")
        self.events.publish(AgentEvent(**self._identity.model_dump(), kind=kind, detail=detail))
