from __future__ import annotations

import collections
import hashlib
import json
import os
import stat
import subprocess
import unittest
from pathlib import Path


COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COOKBOOK_ROOT.parents[2]
README = COOKBOOK_ROOT / "README.md"
RUNNER = COOKBOOK_ROOT / "run.sh"
FINDING = REPOSITORY_ROOT / (
    "docs/findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md"
)
TOPOLOGY = REPOSITORY_ROOT / "benchmarks/agent-durability/topology"
EVIDENCE = TOPOLOGY / "evidence/recovery-20260811-v7"
CASES = (
    "crash-recovery-boundaries",
    "layered-retry-amplification",
    "outage-backlog-herd-recovery",
    "backpressure-overload",
    "poison-work-isolation",
    "silent-progress",
)
BOUNDS = {
    "requests_per_item_max": 4,
    "retry_concurrency_max": 2,
    "in_flight_max": 8,
    "poison_attempts_max": 3,
    "progress_deadline_ms": 5000,
}


def load_json(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise TypeError(f"{path} is not a JSON object")
    return value


class CookbookContractTests(unittest.TestCase):
    def test_readme_states_the_complete_experiment_contract(self) -> None:
        text = README.read_text(encoding="utf-8")

        for heading in (
            "## Question",
            "## Invariant",
            "## Failure boundary",
            "## Oracle",
            "## Fresh-checkout run",
            "## Evidence",
            "## Observed result",
            "## Responsibility split",
            "## Falsifier",
        ):
            self.assertIn(heading, text)

        for required_term in (
            "stable identity",
            "retry owner",
            "deterministic jitter",
            "catch-up",
            "backpressure",
            "quarantine",
            "progress deadline",
            "declared wait",
            "per-item",
            "not a topology performance",
            "source-pinned",
            "not current-source",
        ):
            self.assertIn(required_term, text)

    def test_readme_citations_and_runner_targets_resolve(self) -> None:
        text = README.read_text(encoding="utf-8")

        self.assertTrue(FINDING.is_file())
        self.assertTrue((TOPOLOGY / "README.md").is_file())
        self.assertIn("../../../docs/findings/0016-recovery", text)
        self.assertIn("../../../benchmarks/agent-durability/topology", text)
        self.assertIn("topology-recovery-conformance", text)
        self.assertTrue(RUNNER.is_file())
        self.assertTrue(os.access(RUNNER, os.X_OK))

    def test_admitted_root_is_exact_inventory_sealed_and_distinguishing(self) -> None:
        all_entries = sorted(EVIDENCE.iterdir())
        for path in all_entries:
            self.assertFalse(path.is_symlink(), path)
            self.assertTrue(stat.S_ISDIR(path.lstat().st_mode), path)
            self.assertEqual(path.resolve().parent, EVIDENCE.resolve())
        runs = all_entries
        self.assertEqual(len(runs), 52)

        combinations: collections.Counter[tuple[str, str, str]] = collections.Counter()
        outcomes: collections.Counter[tuple[str, str, str, str, str]] = collections.Counter()
        for run in runs:
            entries = sorted(path.name for path in run.iterdir())
            self.assertEqual(len(entries), 15, run)
            for artifact in run.iterdir():
                self.assertFalse(artifact.is_symlink(), artifact)
                self.assertTrue(stat.S_ISREG(artifact.lstat().st_mode), artifact)

            inventory = load_json(run / "publication-inventory.json")
            hashes = inventory["sha256"]
            self.assertIsInstance(hashes, dict)
            self.assertEqual(set(hashes), set(entries) - {"publication-inventory.json"})
            for relative, expected_digest in hashes.items():
                self.assertNotIn("/", relative)
                self.assertNotIn("\\", relative)
                self.assertEqual(relative, Path(relative).name)
                artifact = run / relative
                self.assertFalse(artifact.is_symlink(), artifact)
                self.assertTrue(stat.S_ISREG(artifact.lstat().st_mode), artifact)
                self.assertEqual(artifact.resolve().parent, run.resolve())
                digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
                self.assertEqual(digest, expected_digest, run / relative)

            effective = load_json(run / "effective-input.json")
            verdict = load_json(run / "verdict.json")
            history = load_json(run / "native-history-or-journal-export.json")
            workload = load_json(run / "workload-state.json")
            case = str(effective["case_id"])
            probe = str(effective["probe"])
            topology = str(effective["topology"])
            combinations[(case, probe, topology)] += 1
            outcomes[
                (
                    str(verdict["admission"]),
                    str(verdict["correctness"]),
                    str(verdict["safety"]),
                    str(verdict["liveness"]),
                    str(verdict["diagnosability"]),
                )
            ] += 1
            self.assertEqual(effective["fanout"], 32)
            self.assertTrue(history["captured"])
            self.assertTrue(history["replay_compatible"])

            recovery = workload["recovery_dynamics"]
            self.assertIsInstance(recovery, dict)
            items = recovery["items"]
            self.assertEqual(len(items), 32)
            self.assertTrue(
                all(item["disposition"] in {"succeeded", "quarantined"} for item in items)
            )
            observed_bounds = {
                item["name"]: item["value"] for item in recovery["bounds"]
            }
            self.assertEqual(observed_bounds, BOUNDS)

        for case in CASES[1:]:
            for probe in ("unfaulted", "unsafe", "protected"):
                for topology in ("direct-activity", "child-workflow"):
                    self.assertEqual(combinations[(case, probe, topology)], 1)
        for topology in ("direct-activity", "child-workflow"):
            self.assertEqual(combinations[(CASES[0], "unfaulted", topology)], 1)
            self.assertEqual(combinations[(CASES[0], "unsafe", topology)], 5)
            self.assertEqual(combinations[(CASES[0], "protected", topology)], 5)

        self.assertEqual(outcomes[("valid", "pass", "pass", "pass", "pass")], 32)
        self.assertEqual(outcomes[("valid", "pass", "fail", "pass", "pass")], 20)

    def test_check_mode_is_read_only_and_cwd_independent(self) -> None:
        if os.environ.get("COOKBOOK_CHECK_MODE") == "1":
            self.skipTest("already executing through run.sh check")
        completed = subprocess.run(
            [str(RUNNER), "check"],
            cwd=Path("/"),
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
