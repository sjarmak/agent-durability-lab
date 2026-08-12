from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

import temporalio
from temporalio.testing import WorkflowEnvironment

from .barrier import BarrierServer
from .product_contract import Arm, ProductScenario
from .product_evidence import (
    SCHEMA,
    ProductExperimentReport,
    audit_product_evidence,
    build_product_environment,
    preserve_product_failure,
    preserve_product_report,
    preserve_product_trial,
)
from .product_metrics import measure_recovery_surface
from .product_population import audit_product_population
from .product_runner import run_product_trial
from .run_experiment import _create_output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sdk-python-root", type=Path, required=True)
    return parser


async def run_product_experiment(output: Path, sdk_python_root: Path) -> ProductExperimentReport:
    _create_output(output)
    project_root = Path(__file__).parents[1]
    repo_root = project_root.parents[1]
    imported_sdk = Path(temporalio.__file__).resolve(strict=True)
    if not imported_sdk.is_relative_to(sdk_python_root.resolve(strict=True)):
        raise ValueError("the running temporalio package is not the pinned SDK tree")
    environment = build_product_environment(repo_root, project_root, sdk_python_root, output.name)
    trials = []
    try:
        temporal_environment = await WorkflowEnvironment.start_local(
            dev_server_existing_path=environment.temporal_cli_path
        )
        with tempfile.TemporaryDirectory(prefix="workflow-stream-product-barrier-") as temporary:
            async with (
                temporal_environment as service,
                BarrierServer(Path(temporary) / "barrier.sock") as barrier,
            ):
                for arm in Arm:
                    for scenario in ProductScenario:
                        for trial_number in range(1, 4):
                            capture = await run_product_trial(
                                client=service.client,
                                project_root=project_root,
                                barrier=barrier,
                                arm=arm,
                                scenario=scenario,
                                trial=trial_number,
                                run_label=output.name,
                            )
                            trials.append(preserve_product_trial(output, capture))
        summary = audit_product_population(tuple((trial.record, trial.verdict) for trial in trials))
        report = ProductExperimentReport(
            SCHEMA,
            environment,
            measure_recovery_surface(project_root),
            summary,
            tuple(trials),
        )
        preserve_product_report(output, report)
        await audit_product_evidence(output, repo_root, project_root)
        return report
    except BaseException as error:
        preserve_product_failure(output, error)
        raise


def main() -> None:
    args = _parser().parse_args()
    report = asyncio.run(run_product_experiment(args.output, args.sdk_python_root))
    print(
        json.dumps(
            {
                "schema": report.schema,
                "runs": report.summary.trials,
                "raw_duplicate_trials": report.summary.raw_duplicate_trials,
                "raw_stale_ack_trials": report.summary.raw_stale_ack_trials,
                "maximum_product_batch_excess_over_manual": (
                    report.summary.maximum_product_batch_excess_over_manual
                ),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
