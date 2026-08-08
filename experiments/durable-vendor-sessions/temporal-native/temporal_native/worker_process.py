"""External Worker process controls shared by live trials and process tests."""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkerProcess:
    process: asyncio.subprocess.Process
    worker_id: str
    pid: int

    @property
    def identity(self) -> str:
        return f"{self.worker_id}/pid-{self.pid}"


async def launch_worker(
    *,
    project_root: Path,
    address: str,
    task_queue: str,
    database_path: Path,
    workspace_path: Path,
    worker_id: str,
    barrier_address: str = "",
    barrier_points: tuple[str, ...] = (),
) -> WorkerProcess:
    command = [
        sys.executable,
        "-m",
        "temporal_native.worker",
        "--address",
        address,
        "--task-queue",
        task_queue,
        "--database",
        str(database_path),
        "--workspace",
        str(workspace_path),
        "--worker-id",
        worker_id,
    ]
    if barrier_address:
        command.extend(("--barrier-address", barrier_address))
    for point in barrier_points:
        command.extend(("--barrier-point", point))
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=project_root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
    if not ready_line:
        stderr = await process.stderr.read() if process.stderr is not None else b""
        raise RuntimeError(f"worker exited before ready: {stderr.decode()}")
    ready = json.loads(ready_line)
    if ready.get("event") != "worker_ready" or ready.get("worker_id") != worker_id:
        raise RuntimeError(f"unexpected worker readiness record: {ready}")
    return WorkerProcess(process=process, worker_id=worker_id, pid=ready["pid"])


async def stop_worker(worker: WorkerProcess, *, kill: bool) -> None:
    if worker.process.returncode is not None:
        return
    if kill:
        worker.process.kill()
    else:
        worker.process.terminate()
    await asyncio.wait_for(worker.process.wait(), timeout=10)
