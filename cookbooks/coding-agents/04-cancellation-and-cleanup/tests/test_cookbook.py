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
    "docs/findings/0006-cancellation-requires-application-revocation.md"
)
EXPERIMENT = REPOSITORY_ROOT / "experiments/cancellation"
EVIDENCE = EXPERIMENT / "evidence"
SCENARIOS = (
    "temporal-control",
    "healthy-safe",
    "worker-death-safe",
    "frozen-safe",
)
TREE_FILE_COUNT = 222
TREE_SHA256 = "158c62df3969e582b37c25752190551853f2be4fa440fc3ffd5933439abdea7a"


def load_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"artifact is not a regular non-symlink file: {path}")
    with path.open("r", encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise TypeError(f"{path} is not a JSON object")
    return value


def sealed_portable_digest(runs: list[Path]) -> tuple[int, str]:
    digest = hashlib.sha256()
    files: list[Path] = []
    for run in runs:
        if run.is_symlink() or not stat.S_ISDIR(run.lstat().st_mode):
            raise ValueError(f"run is not a real directory: {run}")
        if run.resolve().parent != EVIDENCE.resolve():
            raise ValueError(f"run escapes the evidence root: {run}")
        for path in sorted(run.rglob("*")):
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise ValueError(f"symlink in sealed evidence: {path}")
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise ValueError(f"special file in sealed evidence: {path}")
            if path.suffix != ".db":
                files.append(path)
    for path in files:
        data = path.read_bytes()
        relative = path.relative_to(EVIDENCE).as_posix()
        digest.update(
            f"{relative}\0{len(data)}\0{hashlib.sha256(data).hexdigest()}\n".encode()
        )
    return len(files), digest.hexdigest()


def load_json_lines(path: Path) -> list[dict[str, object]]:
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"artifact is not a regular non-symlink file: {path}")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


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
            "WaitForCancellation",
            "application revocation",
            "disconnected",
            "generation",
            "acknowledgement",
            "process tree",
            "stale stop",
            "destination",
        ):
            self.assertIn(required_term, text)

    def test_readme_citations_and_runner_targets_resolve(self) -> None:
        text = README.read_text(encoding="utf-8")

        self.assertTrue(FINDING.is_file())
        self.assertTrue((EXPERIMENT / "README.md").is_file())
        self.assertIn("../../../docs/findings/0006-cancellation", text)
        self.assertIn("../../../experiments/cancellation", text)
        self.assertIn("internal/agentprocess", text)
        self.assertIn("internal/workstore", text)
        self.assertTrue(RUNNER.is_file())
        self.assertTrue(os.access(RUNNER, os.X_OK))

    def test_admitted_matrix_is_exact_and_distinguishing(self) -> None:
        expected = {
            f"cancellation-20260807-v2-{scenario}-wait-{wait}-trial-{trial}"
            for scenario in SCENARIOS
            for wait in ("false", "true")
            for trial in range(1, 4)
        }
        actual = {
            path.name
            for path in EVIDENCE.glob("cancellation-20260807-v2-*")
            if path.is_dir()
        }
        self.assertEqual(actual, expected)
        runs = [EVIDENCE / name for name in sorted(expected)]
        file_count, tree_digest = sealed_portable_digest(runs)
        self.assertEqual(file_count, TREE_FILE_COUNT)
        self.assertEqual(tree_digest, TREE_SHA256)

        control_failures = 0
        safe_passes = 0
        for name in sorted(expected):
            run = EVIDENCE / name
            required = (
                "manifest.json",
                "verdict.json",
                "application-state.json",
                "boundary-state.json",
                "events.jsonl",
                "temporal-history.json",
            )
            for relative in required:
                artifact = run / relative
                self.assertTrue(artifact.is_file(), artifact)
                self.assertFalse(artifact.is_symlink(), artifact)

            manifest = load_json(run / "manifest.json")
            verdict = load_json(run / "verdict.json")
            state = load_json(run / "application-state.json")
            boundary = load_json(run / "boundary-state.json")
            history = load_json(run / "temporal-history.json")
            events = load_json_lines(run / "events.jsonl")
            event_types = collections.Counter(
                event["event_type"] for event in history["events"]
            )
            event_kinds = [event["kind"] for event in events]
            self.assertEqual(
                [event["sequence"] for event in events],
                list(range(1, len(events) + 1)),
            )
            self.assertTrue(verdict["run_valid"])
            self.assertTrue(verdict["expected_observation"])
            self.assertEqual(
                event_types["EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED"], 1
            )
            self.assertEqual(event_types["EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED"], 1)
            self.assertEqual(event_types["EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED"], 1)
            expected_activity_canceled = int(manifest["wait_for_cancellation"])
            self.assertEqual(
                event_types["EVENT_TYPE_ACTIVITY_TASK_CANCELED"],
                expected_activity_canceled,
            )

            if manifest["scenario"] == "temporal-control":
                control_failures += 1
                self.assertFalse(verdict["invariant_satisfied"])
                self.assertIsNone(state.get("cancellation"))
                self.assertEqual(len(state["effects"]), 1)
                self.assertIn("effect_accepted", event_kinds)
                self.assertIn("outcome_accepted", event_kinds)
                self.assertNotIn("cancellation_committed", event_kinds)
            else:
                safe_passes += 1
                self.assertTrue(verdict["invariant_satisfied"])
                cancellation = state["cancellation"]
                self.assertIsNotNone(cancellation)
                self.assertFalse(state["effects"])
                ordered = (
                    "cancellation_committed",
                    "executor_stop_delivery_attempted",
                    "executor_stop_delivery_sent",
                    "executor_stop_received",
                    "cancellation_acknowledged",
                )
                positions = [event_kinds.index(kind) for kind in ordered]
                self.assertEqual(positions, sorted(positions))
                dispositions = [
                    event
                    for event in events
                    if event["kind"] == "process_disposition_observed"
                ]
                self.assertEqual(len(dispositions), 2)
                self.assertTrue(
                    all(event["details"]["disposition"] == "gone" for event in dispositions)
                )
                self.assertIn("tool_child_stop_received", event_kinds)
                target = cancellation["target"]
                acknowledgement = cancellation["acknowledgement"]
                self.assertEqual(cancellation["generation"], target["generation"])
                self.assertEqual(cancellation["generation"], acknowledgement["generation"])
                self.assertEqual(cancellation["owner_token_hash"], target["owner_token_hash"])
                self.assertEqual(cancellation["owner_token_hash"], acknowledgement["owner_token_hash"])
                self.assertEqual(target["process"], acknowledgement["process"])
                self.assertEqual(boundary["store"]["session_id"], target["session_id"])
                if manifest["scenario"] == "worker-death-safe":
                    self.assertLess(
                        event_kinds.index("worker_killed"),
                        event_kinds.index("activity_reattached"),
                    )
                    self.assertLess(
                        event_kinds.index("activity_reattached"),
                        event_kinds.index("cancellation_committed"),
                    )
                if manifest["scenario"] == "frozen-safe":
                    self.assertLess(
                        event_kinds.index("process_tree_frozen"),
                        event_kinds.index("cancellation_committed"),
                    )
                    self.assertLess(
                        event_kinds.index("executor_stop_delivery_sent"),
                        event_kinds.index("process_tree_resumed"),
                    )
                    self.assertLess(
                        event_kinds.index("process_tree_resumed"),
                        event_kinds.index("executor_stop_received"),
                    )

        self.assertEqual(control_failures, 6)
        self.assertEqual(safe_passes, 18)

    def test_check_mode_is_read_only_and_cwd_independent(self) -> None:
        if os.environ.get("COOKBOOK_CHECK_MODE") == "1":
            self.skipTest("already executing through run.sh check")
        completed = subprocess.run(
            [str(RUNNER), "check"],
            cwd=Path("/"),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
