from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

from temporalio.testing import WorkflowEnvironment

from .barrier import BarrierServer
from .contract import Scenario
from .evidence import (
    SCHEMA,
    ExperimentReport,
    audit_evidence,
    build_environment,
    preserve_failure,
    preserve_report,
    preserve_trial,
)
from .runner import run_trial


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _validate_output(path: Path) -> None:
    if path.is_absolute() or ".." in path.parts or "\\" in str(path) or path.name in {"", "."}:
        raise ValueError("output must be a confined relative directory")
    current = Path.cwd()
    for part in path.parts[:-1]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            break
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError("output ancestor must be a real directory")


def _create_output(path: Path) -> None:
    _validate_output(path)
    descriptor = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for index, part in enumerate(path.parts):
            final = index == len(path.parts) - 1
            try:
                os.mkdir(part, 0o750, dir_fd=descriptor)
            except FileExistsError:
                if final:
                    raise
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
    finally:
        os.close(descriptor)


async def run(output: Path) -> ExperimentReport:
    _create_output(output)
    project_root = Path(__file__).parents[1]
    temporal = shutil.which("temporal")
    if temporal is None:
        raise RuntimeError("Temporal CLI is required")
    temporal_version = subprocess.run(
        [temporal, "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    environment = build_environment(project_root, output.name, temporal_version, Path(temporal))
    trials = []
    try:
        temporal_environment = await WorkflowEnvironment.start_local()
        with tempfile.TemporaryDirectory(prefix="workflow-stream-barrier-") as temporary:
            async with (
                temporal_environment as service,
                BarrierServer(Path(temporary) / "barrier.sock") as barrier,
            ):
                for scenario in Scenario:
                    for trial_number in range(1, 4):
                        capture = await run_trial(
                            client=service.client,
                            project_root=project_root,
                            barrier=barrier,
                            scenario=scenario,
                            trial=trial_number,
                            run_label=output.name,
                        )
                        trials.append(preserve_trial(output, capture))
        report = ExperimentReport(SCHEMA, environment, tuple(trials))
        preserve_report(output, report)
        await audit_evidence(output, project_root)
        return report
    except BaseException as error:
        preserve_failure(output, error)
        raise


def main() -> None:
    args = _parser().parse_args()
    report = asyncio.run(run(args.output))
    print(json.dumps({"schema": report.schema, "runs": len(report.trials)}, sort_keys=True))


if __name__ == "__main__":
    main()
