"""External Worker process for live Temporal-native agent trials."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from agents import set_tracing_disabled
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
)
from temporalio.contrib.openai_agents.testing import TestModelProvider
from temporalio.worker import Worker

from temporal_native.activities import TOOL_EFFECT_COMMITTED, ToolActivities, ToolRuntime
from temporal_native.model import MODEL_RESPONSE_BUILT, FixtureModel, ModelRuntime
from temporal_native.workflow import TemporalNativeAgentWorkflow

SUPPORTED_BARRIERS = frozenset((MODEL_RESPONSE_BUILT, TOOL_EFFECT_COMMITTED))
MODEL_EVENTS_TOPIC = "model-events"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", required=True)
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--barrier-address", default="")
    parser.add_argument(
        "--barrier-point", action="append", default=[], choices=sorted(SUPPORTED_BARRIERS)
    )
    return parser


async def run_worker(args: argparse.Namespace) -> None:
    barrier_points = frozenset(args.barrier_point)
    if barrier_points and not args.barrier_address:
        raise ValueError("enabled barriers require --barrier-address")
    set_tracing_disabled(True)
    model = FixtureModel(
        ModelRuntime(
            worker_id=args.worker_id,
            barrier_address=args.barrier_address,
            barrier_points=barrier_points,
        )
    )
    tools = ToolActivities(
        ToolRuntime(
            database_path=args.database.resolve(),
            workspace_path=args.workspace.resolve(),
            worker_id=args.worker_id,
            barrier_address=args.barrier_address,
            barrier_points=barrier_points,
        )
    )
    client = await Client.connect(args.address, plugins=[_build_plugin(model)])
    await _serve_worker(client, tools, args.task_queue, args.worker_id)


def _build_plugin(model: FixtureModel) -> OpenAIAgentsPlugin:
    return OpenAIAgentsPlugin(
        model_provider=TestModelProvider(model),
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=2),
            schedule_to_close_timeout=timedelta(seconds=15),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100),
                maximum_interval=timedelta(seconds=1),
                maximum_attempts=4,
            ),
            streaming_topic=MODEL_EVENTS_TOPIC,
        ),
        add_temporal_spans=False,
    )


async def _serve_worker(
    client: Client,
    tools: ToolActivities,
    task_queue: str,
    worker_id: str,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for process_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(process_signal, stop.set)

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[TemporalNativeAgentWorkflow],
        activities=[tools.apply_fixture_change, tools.record_cleanup],
    )
    try:
        async with worker:
            print(
                json.dumps(
                    {
                        "event": "worker_ready",
                        "worker_id": worker_id,
                        "pid": os.getpid(),
                        "task_queue": task_queue,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            await stop.wait()
    finally:
        client.close()


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
