"""Stateless deterministic model used to calibrate the durable agent loop."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from agents import (
    AgentOutputSchemaBase,
    Handoff,
    Model,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    Tool,
    TResponseInputItem,
)
from agents.items import TResponseStreamEvent
from openai.types.responses import Response, ResponseCompletedEvent
from temporalio import activity
from temporalio.contrib.openai_agents.testing import ResponseBuilders

from temporal_native.barrier import BarrierArrival, BarrierClient
from temporal_native.contract import (
    AgentToolInput,
    AgentTurnEnvelope,
    DestinationReceipt,
    TurnResult,
)

MODEL_RESPONSE_BUILT = "model-response-built"


@dataclass(frozen=True)
class ModelRuntime:
    """Worker identity and optional exact fault controller."""

    worker_id: str
    barrier_address: str = ""
    barrier_points: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ValueError("worker identity is required")


class FixtureModel(Model):
    """Chooses one tool structurally, then returns its receipt as typed output."""

    def __init__(self, runtime: ModelRuntime) -> None:
        self._runtime = runtime

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> ModelResponse:
        """Return a tool call before tool output and a final result after it."""

        del system_instructions, model_settings, tools, output_schema, handoffs, tracing, kwargs
        return await self._build_response(input)

    async def _build_response(self, input: str | list[TResponseInputItem]) -> ModelResponse:
        receipt = _find_tool_receipt(input)
        if receipt is None:
            envelope = _find_turn_envelope(input)
            tool_input = AgentToolInput(
                session_id=envelope.session_id,
                logical_turn_id=envelope.logical_turn_id,
                logical_effect_id=envelope.logical_effect_id,
                generation=envelope.generation,
                owner_capability=envelope.owner_capability,
                relative_path=envelope.relative_path,
                content=envelope.content,
            )
            response = ResponseBuilders.tool_call(
                json.dumps({"tool_input": tool_input.model_dump(mode="json")}),
                "apply_fixture_change",
            )
            identity = envelope
        else:
            result = TurnResult(
                session_id=receipt.session_id,
                logical_turn_id=receipt.logical_turn_id,
                logical_effect_id=receipt.logical_effect_id,
                generation=receipt.generation,
                owner_capability=receipt.owner_capability,
                artifact_sha256=receipt.artifact_sha256,
                destination_receipt=receipt.receipt_id,
            )
            response = ResponseBuilders.output_message(result.model_dump_json())
            identity = receipt

        if MODEL_RESPONSE_BUILT in self._runtime.barrier_points:
            await self._arrive(identity)
        return response

    def stream_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> AsyncIterator[TResponseStreamEvent]:
        del system_instructions, model_settings, tools, output_schema, handoffs, tracing, kwargs
        return self._stream_built_response(input)

    async def _stream_built_response(
        self, input: str | list[TResponseInputItem]
    ) -> AsyncIterator[TResponseStreamEvent]:
        response = await self._build_response(input)
        yield ResponseCompletedEvent(
            response=Response(
                id="fixture-response",
                created_at=0.0,
                model="deterministic-fixture",
                object="response",
                output=response.output,
                parallel_tool_calls=False,
                tool_choice="auto",
                tools=[],
                status="completed",
            ),
            sequence_number=0,
            type="response.completed",
        )

    async def _arrive(self, identity: AgentTurnEnvelope | DestinationReceipt) -> None:
        if not self._runtime.barrier_address:
            raise RuntimeError("model fault barrier is enabled without a controller")
        info = activity.info()
        activity_attempt_id = "/".join(
            (
                info.workflow_run_id or "missing-run",
                info.activity_id,
                "attempt",
                str(info.attempt),
            )
        )
        await BarrierClient(self._runtime.barrier_address).arrive(
            BarrierArrival(
                point=MODEL_RESPONSE_BUILT,
                session_id=identity.session_id,
                logical_turn_id=identity.logical_turn_id,
                logical_effect_id=identity.logical_effect_id,
                activity_attempt=info.attempt,
                worker_process=f"{self._runtime.worker_id}/pid-{os.getpid()}",
                arrival_token=f"{activity_attempt_id}/barrier/{MODEL_RESPONSE_BUILT}",
            )
        )


def _find_turn_envelope(input: str | list[TResponseInputItem]) -> AgentTurnEnvelope:
    for value in _string_values(input):
        try:
            return AgentTurnEnvelope.model_validate_json(value)
        except ValueError:
            continue
    raise ValueError("model input lacks a valid Workflow-authored turn envelope")


def _find_tool_receipt(
    input: str | list[TResponseInputItem],
) -> DestinationReceipt | None:
    if isinstance(input, str):
        return None
    for item in input:
        if _field(item, "type") != "function_call_output":
            continue
        output = _field(item, "output")
        for value in _string_values(output):
            try:
                return DestinationReceipt.model_validate_json(value)
            except ValueError:
                continue
        raise ValueError("tool output lacks a valid destination receipt")
    return None


def _field(value: object, name: str) -> object | None:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _string_values(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for nested in value.values():
            strings.extend(_string_values(nested))
        return strings
    if isinstance(value, (list, tuple)):
        strings = []
        for nested in value:
            strings.extend(_string_values(nested))
        return strings
    if hasattr(value, "model_dump_json"):
        encoded = value.model_dump_json()
        return [encoded] if json.loads(encoded) else []
    return []
