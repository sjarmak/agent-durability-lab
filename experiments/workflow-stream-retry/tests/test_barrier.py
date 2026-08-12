from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from workflow_stream_retry.barrier import BarrierArrival, BarrierClient, BarrierServer


async def test_barrier_requires_exact_single_use_expectation(tmp_path: Path) -> None:
    async with BarrierServer(tmp_path / "barrier.sock") as server:
        expected = BarrierArrival(
            point="post-flush",
            workflow_id="workflow-1",
            attempt=1,
            worker_id="worker-1",
        )
        server.expect(expected)
        client = BarrierClient(server.socket_path, server.credential)
        arrival_task = asyncio.create_task(client.arrive(expected))
        assert await asyncio.wait_for(server.next_arrival(), timeout=1) == expected
        server.release(expected)
        await asyncio.wait_for(arrival_task, timeout=1)

        with pytest.raises(RuntimeError):
            await client.arrive(expected)


async def test_barrier_rejects_wrong_identity_without_burning_expectation(tmp_path: Path) -> None:
    async with BarrierServer(tmp_path / "barrier.sock") as server:
        expected = BarrierArrival("pre-flush", "workflow-1", 1, "worker-1")
        server.expect(expected)
        client = BarrierClient(server.socket_path, server.credential)
        with pytest.raises(RuntimeError):
            await client.arrive(BarrierArrival("pre-flush", "workflow-1", 1, "attacker"))

        arrival_task = asyncio.create_task(client.arrive(expected))
        assert await asyncio.wait_for(server.next_arrival(), timeout=1) == expected
        server.release(expected)
        await arrival_task


async def test_barrier_rejects_duplicate_keys_without_burning_expectation(tmp_path: Path) -> None:
    async with BarrierServer(tmp_path / "barrier.sock") as server:
        expected = BarrierArrival("pre-flush", "workflow-1", 1, "worker-1")
        server.expect(expected)
        reader, writer = await asyncio.open_unix_connection(server.socket_path)
        request = json.dumps(
            {"credential": server.credential, "arrival": expected.__dict__},
            separators=(",", ":"),
        )
        request = request.replace('"point":"pre-flush"', '"point":"attacker","point":"pre-flush"')
        writer.write(request.encode() + b"\n")
        await writer.drain()
        response = json.loads(await reader.readline())
        writer.close()
        await writer.wait_closed()
        assert "duplicate JSON key" in response["error"]

        arrival_task = asyncio.create_task(
            BarrierClient(server.socket_path, server.credential).arrive(expected)
        )
        assert await asyncio.wait_for(server.next_arrival(), timeout=1) == expected
        server.release(expected)
        await arrival_task


async def test_barrier_rejects_symlinked_socket_ancestor(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        async with BarrierServer(alias / "barrier.sock"):
            pass
