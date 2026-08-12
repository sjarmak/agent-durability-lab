from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from .activity import ActivityRuntime, PublisherActivities
from .barrier import BarrierClient
from .workflow import StreamRetryWorkflow


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--worker-id", required=True)
    return parser


async def _run() -> None:
    args = _parser().parse_args()
    socket = os.environ.get("WORKFLOW_STREAM_BARRIER_SOCKET", "")
    credential_fd = os.environ.get("WORKFLOW_STREAM_BARRIER_CREDENTIAL_FD", "")
    credential = ""
    if credential_fd:
        descriptor = int(credential_fd)
        try:
            credential = os.read(descriptor, 65).decode("ascii")
        finally:
            os.close(descriptor)
    barrier = BarrierClient(Path(socket), credential) if socket or credential else None
    client = await Client.connect(args.address)
    process_identity = f"{args.worker_id}/pid-{os.getpid()}"
    activities = PublisherActivities(ActivityRuntime(process_identity, barrier))
    async with Worker(
        client,
        task_queue=args.task_queue,
        workflows=[StreamRetryWorkflow],
        activities=[activities.publish],
        identity=process_identity,
    ):
        print(
            json.dumps({"event": "worker_ready", "worker_id": args.worker_id, "pid": os.getpid()}),
            flush=True,
        )
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(_run())
