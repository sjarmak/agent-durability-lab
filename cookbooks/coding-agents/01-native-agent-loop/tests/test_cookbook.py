from __future__ import annotations

import json
import hashlib
import stat
import subprocess
import unittest
from pathlib import Path


COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COOKBOOK_ROOT.parents[2]
README = COOKBOOK_ROOT / "README.md"
RUNNER = COOKBOOK_ROOT / "run.sh"
FINDING = REPOSITORY_ROOT / (
    "docs/findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md"
)
EXPERIMENT = REPOSITORY_ROOT / "experiments/durable-vendor-sessions/temporal-native"
EVIDENCE = EXPERIMENT / "evidence/temporal-native-20260807-v3"
TREE_FILE_COUNT = 54
TREE_SHA256 = "fb055c2f8c4e2272e452320dab6d6df4c0d2def899b631b42bc54cf5c155e1fd"
TRIAL_ARTIFACTS = {
    "authority-state.json",
    "common-events.jsonl",
    "destination-state.json",
    "effective-input.json",
    "fault-boundary.json",
    "manifest.json",
    "native-history-or-journal-export.json",
    "process-observations.json",
    "verdict.json",
}


def regular_files_and_digest(root: Path) -> tuple[list[Path], str]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"evidence root is not a real directory: {root}")
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise ValueError(f"symlink in sealed evidence: {path}")
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError(f"special file in sealed evidence: {path}")
        files.append(path)
    digest = hashlib.sha256()
    for path in files:
        data = path.read_bytes()
        relative = path.relative_to(root).as_posix()
        record = f"{relative}\0{len(data)}\0{hashlib.sha256(data).hexdigest()}\n"
        digest.update(record.encode("utf-8"))
    return files, digest.hexdigest()


def load_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"artifact is not a regular non-symlink file: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
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
            "model Activities",
            "tool Activities",
            "approval",
            "typed result",
            "stream state",
            "Continue-As-New",
            "experimental boundary",
            "ambiguous",
            "unsafe",
            "destination-idempotent",
            "history replay",
        ):
            self.assertIn(required_term, text)

    def test_readme_citations_resolve_to_admitted_sources(self) -> None:
        text = README.read_text(encoding="utf-8")

        self.assertTrue(FINDING.is_file())
        self.assertTrue((EXPERIMENT / "README.md").is_file())
        self.assertTrue(EVIDENCE.is_dir())
        self.assertIn("../../../docs/findings/0008-temporal-native-agent-loop", text)
        self.assertIn(
            "../../../experiments/durable-vendor-sessions/temporal-native", text
        )
        self.assertNotIn("temporal-native-20260807-v1", text)
        self.assertNotIn("temporal-native-20260807-v2", text)

    def test_admitted_evidence_retains_unsafe_control_and_protected_trials(self) -> None:
        files, tree_digest = regular_files_and_digest(EVIDENCE)
        self.assertEqual(len(files), TREE_FILE_COUNT)
        self.assertEqual(tree_digest, TREE_SHA256)
        unsafe = sorted(EVIDENCE.glob("*-unsafe-trial-*"))
        protected = sorted(EVIDENCE.glob("*-protected-trial-*"))

        self.assertEqual(len(unsafe), 3)
        self.assertEqual(len(protected), 3)
        for trial in unsafe + protected:
            self.assertFalse(trial.is_symlink(), trial)
            self.assertEqual(trial.resolve().parent, EVIDENCE.resolve())
            self.assertEqual({path.name for path in trial.iterdir()}, TRIAL_ARTIFACTS)

            manifest = load_json(trial / "manifest.json")
            hashes = manifest["evidence_sha256"]
            self.assertIsInstance(hashes, dict)
            self.assertEqual(set(hashes), TRIAL_ARTIFACTS - {"manifest.json", "verdict.json"})
            for relative, expected in hashes.items():
                self.assertNotIn("/", relative)
                self.assertNotIn("\\", relative)
                self.assertEqual(relative, Path(relative).name)
                artifact = trial / relative
                self.assertFalse(artifact.is_symlink(), artifact)
                self.assertEqual(artifact.resolve().parent, trial.resolve())
                self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(), expected)

            verdict = load_json(trial / "verdict.json")
            destination = load_json(trial / "destination-state.json")
            authority = load_json(trial / "authority-state.json")
            boundary = load_json(trial / "fault-boundary.json")
            events = [json.loads(line) for line in (trial / "common-events.jsonl").read_text(encoding="utf-8").splitlines()]
            native = json.loads((trial / "native-history-or-journal-export.json").read_text(encoding="utf-8"))
            event_kinds = [event["kind"] for event in events]
            self.assertEqual([event["sequence"] for event in events], list(range(1, len(events) + 1)))
            self.assertTrue(boundary["triggered"])
            self.assertLess(boundary["after_sequence"], boundary["before_sequence"])
            self.assertEqual(len(authority["accepted_outcomes"]), 1)
            self.assertEqual(authority["concurrent_owner_count"], 1)
            self.assertEqual(event_kinds.count("outcome_accepted"), 1)
            temporal_types = {
                json.loads(item["detail"])["eventType"]
                for item in native
                if item["kind"] == "temporal_history_event"
            }
            self.assertIn("EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED", temporal_types)
            self.assertIn("EVENT_TYPE_ACTIVITY_TASK_STARTED", temporal_types)

            applied = sum(item["applied"] for item in destination["attempts"])
            if "-unsafe-" in trial.name:
                self.assertEqual(verdict["class"], "valid-fail")
                self.assertIn("duplicate_physical_effect", verdict["reason_codes"])
                self.assertEqual(event_kinds.count("effect_accepted"), 2)
                self.assertEqual(applied, 2)
            else:
                self.assertEqual(verdict["class"], "valid-pass")
                self.assertEqual(event_kinds.count("effect_accepted"), 1)
                self.assertEqual(event_kinds.count("effect_rejected"), 1)
                self.assertEqual(applied, 1)

    def test_runner_exposes_documented_read_only_and_critical_paths(self) -> None:
        result = subprocess.run(
            [str(RUNNER), "--help"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("check", result.stdout)
        self.assertIn("critical", result.stdout)
        self.assertIn("all", result.stdout)
        readme = README.read_text(encoding="utf-8")
        self.assertIn("./cookbooks/coding-agents/01-native-agent-loop/run.sh all", readme)

    def test_critical_path_targets_typed_result_and_history_replay(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")

        self.assertIn(
            "test_agent_loop_correlates_model_tool_destination_and_result", runner
        )
        upstream_test = (EXPERIMENT / "tests/test_workflow.py").read_text(encoding="utf-8")
        self.assertIn("replay_workflow(history)", upstream_test)
        self.assertIn("isinstance(result, TurnResult)", upstream_test)


if __name__ == "__main__":
    unittest.main()
