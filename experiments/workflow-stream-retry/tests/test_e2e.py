from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

import pytest

from workflow_stream_retry.evidence import (
    SCHEMA,
    EvidenceManifest,
    _inventory,
    _jsonable,
    audit_evidence,
)
from workflow_stream_retry.run_experiment import run


async def test_append_only_population_round_trips_through_independent_audit() -> None:
    project_root = Path(__file__).parents[1]
    output = Path(f".workflow-stream-e2e-{uuid.uuid4().hex}")
    try:
        report = await run(output)
        audited = await audit_evidence(output, project_root)
        assert audited == report
        assert len(audited.trials) == 9
        assert sum(trial.verdict.naive_duplicate_control_failed for trial in audited.trials) == 3

        report_path = output / "report.json"
        trial_path = output / "post-flush-duplicate-trial-1-trial.json"
        for path in (report_path, trial_path):
            document = json.loads(path.read_bytes())
            target = document["trials"][6] if path == report_path else document
            target["verdict"]["naive_duplicate_control_failed"] = False
            path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        manifest = EvidenceManifest(SCHEMA, _inventory(output, exclude_manifest=True))
        (output / "manifest.json").write_text(
            json.dumps(_jsonable(manifest), indent=2, sort_keys=True) + "\n"
        )
        with pytest.raises(ValueError):
            await audit_evidence(output, project_root)
    finally:
        shutil.rmtree(output, ignore_errors=True)
