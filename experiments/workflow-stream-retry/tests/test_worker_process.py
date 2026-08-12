from __future__ import annotations

import asyncio
import sys

import pytest

from workflow_stream_retry.worker_process import WorkerProcess, _decode_ready, stop_worker


def test_worker_readiness_pid_binds_launcher_process() -> None:
    valid = b'{"event":"worker_ready","worker_id":"worker","pid":123}\n'
    assert _decode_ready(valid, 123, "worker") == 123
    with pytest.raises(RuntimeError, match="readiness"):
        _decode_ready(b'{"event":"worker_ready","worker_id":"worker","pid":999}\n', 123, "worker")


async def test_stop_worker_force_kills_term_ignoring_process() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import signal,time; "
        "signal.signal(signal.SIGTERM, lambda *_: None); "
        "print('ready', flush=True); time.sleep(60)",
        stdout=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    assert await process.stdout.readline() == b"ready\n"
    await stop_worker(WorkerProcess(process, "worker", process.pid), kill=False, grace_seconds=0.05)
    assert process.returncode == -9


async def test_stop_worker_cancellation_still_force_kills_and_reaps() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import signal,time; "
        "signal.signal(signal.SIGTERM, lambda *_: None); "
        "print('ready', flush=True); time.sleep(60)",
        stdout=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    assert await process.stdout.readline() == b"ready\n"
    cleanup = asyncio.create_task(
        stop_worker(WorkerProcess(process, "worker", process.pid), kill=False)
    )
    await asyncio.sleep(0)
    cleanup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cleanup
    assert process.returncode == -9
