from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from .barrier import BarrierServer, _strict_json


@dataclass(frozen=True)
class WorkerProcess:
    process: asyncio.subprocess.Process
    worker_id: str
    pid: int


async def launch_worker(
    *,
    project_root: Path,
    address: str,
    task_queue: str,
    worker_id: str,
    barrier: BarrierServer | None,
) -> WorkerProcess:
    environment = dict(os.environ)
    environment.pop("WORKFLOW_STREAM_BARRIER_SOCKET", None)
    environment.pop("WORKFLOW_STREAM_BARRIER_CREDENTIAL", None)
    environment.pop("WORKFLOW_STREAM_BARRIER_CREDENTIAL_FD", None)
    credential_read: int | None = None
    if barrier is not None:
        environment["WORKFLOW_STREAM_BARRIER_SOCKET"] = str(barrier.socket_path)
        credential_read, credential_write = os.pipe()
        environment["WORKFLOW_STREAM_BARRIER_CREDENTIAL_FD"] = str(credential_read)
        try:
            os.write(credential_write, barrier.credential.encode("ascii"))
        finally:
            os.close(credential_write)
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "workflow_stream_retry.worker",
            "--address",
            address,
            "--task-queue",
            task_queue,
            "--worker-id",
            worker_id,
            cwd=project_root,
            env=environment,
            pass_fds=() if credential_read is None else (credential_read,),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    finally:
        if credential_read is not None:
            os.close(credential_read)
    try:
        assert process.stdout is not None
        ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
        if not ready_line:
            stderr = await process.stderr.read() if process.stderr is not None else b""
            raise RuntimeError(
                f"worker exited before readiness: {stderr.decode(errors='replace')}"
            )
        ready_pid = _decode_ready(ready_line, process.pid, worker_id)
        return WorkerProcess(process, worker_id, ready_pid)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise


def _decode_ready(data: bytes, process_pid: int, worker_id: str) -> int:
    ready = _strict_json(data)
    if (
        not isinstance(ready, dict)
        or set(ready) != {"event", "worker_id", "pid"}
        or ready.get("event") != "worker_ready"
        or ready.get("worker_id") != worker_id
        or type(ready.get("pid")) is not int
        or ready["pid"] != process_pid
        or process_pid <= 0
    ):
        raise RuntimeError(f"invalid worker readiness record: {ready}")
    return process_pid


async def stop_worker(
    worker: WorkerProcess,
    *,
    kill: bool,
    grace_seconds: float = 10,
) -> None:
    if worker.process.returncode is not None:
        return
    if kill:
        worker.process.kill()
    else:
        worker.process.terminate()
    try:
        await asyncio.wait_for(worker.process.wait(), timeout=grace_seconds)
    except TimeoutError:
        if kill:
            raise
        worker.process.kill()
        try:
            await asyncio.wait_for(worker.process.wait(), timeout=grace_seconds)
        except asyncio.CancelledError:
            await asyncio.shield(worker.process.wait())
            raise
    except asyncio.CancelledError:
        if worker.process.returncode is None:
            worker.process.kill()
        await asyncio.shield(worker.process.wait())
        raise
