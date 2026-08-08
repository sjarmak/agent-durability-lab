from __future__ import annotations

import asyncio

import pytest

from temporal_native.barrier import BarrierArrival, BarrierClient, BarrierServer


def arrival(token: str = "arrival-1") -> BarrierArrival:
    return BarrierArrival(
        point="tool-effect-committed",
        session_id="session-1",
        logical_turn_id="session-1/turn/1",
        logical_effect_id="session-1/turn/1/effect/1",
        activity_attempt=2,
        worker_process="worker-2/pid-123",
        arrival_token=token,
    )


async def test_barrier_blocks_until_controller_releases_exact_arrival() -> None:
    async with BarrierServer() as server:
        blocked = asyncio.create_task(BarrierClient(server.address).arrive(arrival()))

        observed = await server.next_arrival("tool-effect-committed")
        assert observed == arrival()
        assert not blocked.done()

        server.release(observed.arrival_token)
        await asyncio.wait_for(blocked, timeout=1)


async def test_barrier_rejects_unknown_or_reused_arrival_token() -> None:
    async with BarrierServer() as server:
        blocked = asyncio.create_task(BarrierClient(server.address).arrive(arrival()))
        observed = await server.next_arrival("tool-effect-committed")

        with pytest.raises(ValueError, match="unknown arrival"):
            server.release("wrong-token")

        server.release(observed.arrival_token)
        await blocked

        with pytest.raises(ValueError, match="unknown arrival"):
            server.release(observed.arrival_token)


async def test_barrier_routes_arrivals_by_named_point() -> None:
    async with BarrierServer() as server:
        model = arrival("model-1").model_copy(update={"point": "model-response-built"})
        model_blocked = asyncio.create_task(BarrierClient(server.address).arrive(model))
        tool_blocked = asyncio.create_task(BarrierClient(server.address).arrive(arrival()))

        observed_tool = await server.next_arrival("tool-effect-committed")
        observed_model = await server.next_arrival("model-response-built")
        assert observed_tool.arrival_token == "arrival-1"
        assert observed_model.arrival_token == "model-1"

        server.release(observed_tool.arrival_token)
        server.release(observed_model.arrival_token)
        await asyncio.gather(model_blocked, tool_blocked)
