from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import stat
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from types import TracebackType
from typing import cast

MAX_MESSAGE_BYTES = 4096
REQUEST_TIMEOUT_SECONDS = 2


@dataclass(frozen=True)
class BarrierArrival:
    point: str
    workflow_id: str
    attempt: int
    worker_id: str

    def validate(self) -> None:
        if (
            type(self.point) is not str
            or not self.point
            or type(self.workflow_id) is not str
            or not self.workflow_id
            or type(self.attempt) is not int
            or self.attempt < 1
            or type(self.worker_id) is not str
            or not self.worker_id
        ):
            raise ValueError("barrier identity is incomplete")


class BarrierClient:
    def __init__(self, socket_path: Path, credential: str) -> None:
        if not socket_path.is_absolute() or len(credential) != 64:
            raise ValueError("barrier socket and credential are required")
        self._socket_path = socket_path
        self._credential = credential

    async def arrive(self, arrival: BarrierArrival) -> None:
        arrival.validate()
        reader, writer = await asyncio.open_unix_connection(self._socket_path)
        try:
            request = {"credential": self._credential, "arrival": asdict(arrival)}
            writer.write(json.dumps(request, separators=(",", ":")).encode() + b"\n")
            await writer.drain()
            response = await reader.readline()
            if not response or len(response) > MAX_MESSAGE_BYTES:
                raise RuntimeError("barrier closed without an acknowledgement")
            decoded = json.loads(response)
            if decoded != {"released": asdict(arrival)}:
                raise RuntimeError(f"barrier rejected arrival: {decoded}")
        finally:
            writer.close()
            await writer.wait_closed()


class BarrierServer:
    def __init__(self, socket_path: Path) -> None:
        if not socket_path.is_absolute():
            raise ValueError("barrier socket path must be absolute")
        self.socket_path = socket_path
        self.credential = secrets.token_hex(32)
        self._server: asyncio.Server | None = None
        self._expected: set[BarrierArrival] = set()
        self._arrivals: asyncio.Queue[BarrierArrival] = asyncio.Queue()
        self._pending: dict[BarrierArrival, asyncio.Future[None]] = {}

    async def __aenter__(self) -> BarrierServer:
        _reject_symlink_components(self.socket_path.parent)
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise ValueError("barrier socket path already exists")
        self._server = await asyncio.start_unix_server(self._handle, path=self.socket_path)
        os.chmod(self.socket_path, 0o600)
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
        self.socket_path.unlink(missing_ok=True)

    def expect(self, arrival: BarrierArrival) -> None:
        arrival.validate()
        if arrival in self._expected or arrival in self._pending:
            raise ValueError("barrier expectation already registered")
        self._expected.add(arrival)

    async def next_arrival(self) -> BarrierArrival:
        return await self._arrivals.get()

    def release(self, arrival: BarrierArrival) -> None:
        pending = self._pending.get(arrival)
        if pending is None or pending.done():
            raise ValueError("barrier arrival is not pending")
        pending.set_result(None)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        arrival: BarrierArrival | None = None
        release: asyncio.Future[None] | None = None
        disconnect: asyncio.Task[bytes] | None = None
        try:
            request = await asyncio.wait_for(reader.readline(), timeout=REQUEST_TIMEOUT_SECONDS)
            arrival = self._decode_request(request)
            if arrival not in self._expected:
                raise ValueError("barrier identity was not expected")
            self._expected.remove(arrival)
            release = asyncio.get_running_loop().create_future()
            self._pending[arrival] = release
            self._arrivals.put_nowait(arrival)
            disconnect = asyncio.create_task(reader.read(1))
            waiters = {
                cast(asyncio.Future[object], release),
                cast(asyncio.Future[object], disconnect),
            }
            done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            if release not in done:
                return
            await _send_json(writer, {"released": asdict(arrival)})
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError, TimeoutError) as error:
            await _send_json(writer, {"error": str(error)})
        finally:
            if disconnect is not None:
                disconnect.cancel()
            if arrival is not None and self._pending.get(arrival) is release:
                self._pending.pop(arrival, None)
            writer.close()
            with suppress(BrokenPipeError, ConnectionResetError):
                await writer.wait_closed()

    def _decode_request(self, request: bytes) -> BarrierArrival:
        if not request or len(request) > MAX_MESSAGE_BYTES:
            raise ValueError("barrier request is empty or too large")
        decoded = _strict_json(request)
        if not isinstance(decoded, dict) or set(decoded) != {"credential", "arrival"}:
            raise ValueError("barrier request shape is invalid")
        credential = decoded["credential"]
        if not isinstance(credential, str) or not hmac.compare_digest(credential, self.credential):
            raise ValueError("barrier credential is invalid")
        raw_arrival = decoded["arrival"]
        if not isinstance(raw_arrival, dict) or set(raw_arrival) != {
            "point",
            "workflow_id",
            "attempt",
            "worker_id",
        }:
            raise ValueError("barrier arrival shape is invalid")
        arrival = BarrierArrival(**raw_arrival)
        arrival.validate()
        return arrival


def _strict_json(data: bytes) -> object:
    text = data.decode("utf-8")

    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=pairs)


def _reject_symlink_components(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError("barrier socket path contains a symlink")


async def _send_json(writer: asyncio.StreamWriter, value: object) -> None:
    try:
        writer.write(json.dumps(value).encode() + b"\n")
        await writer.drain()
    except (BrokenPipeError, ConnectionResetError):
        return
