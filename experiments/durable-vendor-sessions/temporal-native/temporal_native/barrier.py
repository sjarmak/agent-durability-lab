"""Tokenized request/acknowledgement barriers for exact fault injection."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from types import TracebackType

from pydantic import Field

from temporal_native.contract import StrictModel

MAX_MESSAGE_BYTES = 64 * 1024


class BarrierArrival(StrictModel):
    """Identity-rich observation made while the fault target is blocked."""

    point: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    logical_turn_id: str = Field(min_length=1)
    logical_effect_id: str = Field(min_length=1)
    activity_attempt: int = Field(ge=1)
    worker_process: str = Field(min_length=1)
    arrival_token: str = Field(min_length=1)


class BarrierClient:
    """Activity-side barrier endpoint."""

    def __init__(self, address: str) -> None:
        host, separator, port = address.rpartition(":")
        if not separator or not host or not port.isdigit():
            raise ValueError("barrier address must be host:port")
        self._host = host
        self._port = int(port)

    async def arrive(self, arrival: BarrierArrival) -> None:
        reader, writer = await asyncio.open_connection(self._host, self._port)
        try:
            writer.write(arrival.model_dump_json().encode() + b"\n")
            await writer.drain()
            response = await reader.readline()
            if not response or len(response) > MAX_MESSAGE_BYTES:
                raise RuntimeError("barrier closed without a valid acknowledgement")
            decoded = json.loads(response)
            if decoded != {"released": arrival.arrival_token}:
                raise RuntimeError("barrier returned an invalid acknowledgement")
        finally:
            writer.close()
            await writer.wait_closed()


class BarrierServer:
    """Controller-side router for named, one-use barrier arrivals."""

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._address = ""
        self._queues: defaultdict[str, asyncio.Queue[BarrierArrival]] = defaultdict(asyncio.Queue)
        self._pending: dict[str, asyncio.Future[None]] = {}

    @property
    def address(self) -> str:
        if not self._address:
            raise RuntimeError("barrier server is not running")
        return self._address

    async def __aenter__(self) -> BarrierServer:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        socket = self._server.sockets[0]
        host, port = socket.getsockname()[:2]
        self._address = f"{host}:{port}"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        self._address = ""

    async def next_arrival(self, point: str) -> BarrierArrival:
        if not point:
            raise ValueError("barrier point is required")
        return await self._queues[point].get()

    def release(self, arrival_token: str) -> None:
        pending = self._pending.pop(arrival_token, None)
        if pending is None or pending.done():
            raise ValueError("unknown arrival token")
        pending.set_result(None)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        arrival: BarrierArrival | None = None
        release: asyncio.Future[None] | None = None
        disconnect: asyncio.Task[bytes] | None = None
        try:
            request = await reader.readline()
            if not request or len(request) > MAX_MESSAGE_BYTES:
                raise ValueError("barrier request is empty or too large")
            arrival = BarrierArrival.model_validate_json(request)
            if arrival.arrival_token in self._pending:
                raise ValueError("barrier arrival token is already pending")
            release = asyncio.get_running_loop().create_future()
            self._pending[arrival.arrival_token] = release
            self._queues[arrival.point].put_nowait(arrival)

            disconnect = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait({release, disconnect}, return_when=asyncio.FIRST_COMPLETED)
            if release not in done:
                return
            writer.write(json.dumps({"released": arrival.arrival_token}).encode() + b"\n")
            await writer.drain()
        except (ValueError, json.JSONDecodeError) as error:
            writer.write(json.dumps({"error": str(error)}).encode() + b"\n")
            await writer.drain()
        finally:
            if disconnect is not None:
                disconnect.cancel()
            if arrival is not None and self._pending.get(arrival.arrival_token) is release:
                self._pending.pop(arrival.arrival_token, None)
            writer.close()
            await writer.wait_closed()
