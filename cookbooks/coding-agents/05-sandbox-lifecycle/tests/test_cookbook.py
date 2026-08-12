from __future__ import annotations

import hashlib
import json
import stat
import subprocess
import unittest
from pathlib import Path


COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COOKBOOK_ROOT.parents[2]
README = COOKBOOK_ROOT / "README.md"
RUNNER = COOKBOOK_ROOT / "run.sh"
FINDING = REPOSITORY_ROOT / (
    "docs/findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md"
)
EXPERIMENT = REPOSITORY_ROOT / (
    "experiments/durable-vendor-sessions/sandbox-harness"
)
EVIDENCE = EXPERIMENT / "evidence/sandbox-harness-20260808-v7"
SOURCE_SHA256 = "b775a6142770467158fe6f61b3c16c183ae754731dc551e9ead8cf6f7ea55402"
UPSTREAM_COMMIT = "e8a88540d9523a3d9070860913567670194bacc1"
TREE_FILE_COUNT = 368
TREE_SHA256 = "62cbab0ba33e673845384da52eb82e737085b9b570aeb771c2d326289f62ffee"


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
        digest.update(
            f"{relative}\0{len(data)}\0{hashlib.sha256(data).hexdigest()}\n".encode()
        )
    return files, digest.hexdigest()


def confined_artifact(trial: Path, relative: str) -> Path:
    if (
        not relative
        or "/" in relative
        or "\\" in relative
        or relative in {".", ".."}
        or Path(relative).name != relative
    ):
        raise ValueError(f"unconfined artifact path: {relative!r}")
    artifact = trial / relative
    if artifact.is_symlink() or not stat.S_ISREG(artifact.lstat().st_mode):
        raise ValueError(f"artifact is not a regular non-symlink file: {artifact}")
    if artifact.resolve().parent != trial.resolve():
        raise ValueError(f"artifact escapes trial root: {artifact}")
    return artifact


def read_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"artifact is not a regular non-symlink file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


class CookbookContractTests(unittest.TestCase):
    def test_readme_states_the_complete_experiment_contract(self) -> None:
        text = README.read_text(encoding="utf-8")

        for heading in (
            "## Question",
            "## Invariant",
            "## Failure boundaries",
            "## Oracle",
            "## Fresh-checkout run",
            "## Evidence",
            "## Observed result",
            "## Responsibility split",
            "## Falsifier",
        ):
            self.assertIn(heading, text)

        for required_term in (
            "sandbox ownership",
            "agent-session ownership",
            "stable outer Update ID",
            "provider receipt",
            "create",
            "command",
            "snapshot",
            "stop",
            "stale attached writer",
            "parent-close",
            "workspace prefix",
            "provider journal",
            "history replay",
        ):
            self.assertIn(required_term, text)

    def test_readme_citations_resolve_to_the_admitted_sources(self) -> None:
        text = README.read_text(encoding="utf-8")

        self.assertTrue(FINDING.is_file())
        self.assertTrue((EXPERIMENT / "README.md").is_file())
        self.assertTrue(EVIDENCE.is_dir())
        self.assertIn(
            "../../../docs/findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md",
            text,
        )
        self.assertIn(
            "../../../experiments/durable-vendor-sessions/sandbox-harness/README.md",
            text,
        )
        self.assertIn("sandbox-harness-20260808-v7", text)
        self.assertNotIn("sandbox-harness-20260808-v6", text)

    def test_admitted_matrix_distinguishes_all_ambiguous_provider_operations(
        self,
    ) -> None:
        for operation in ("start", "command", "snapshot", "stop"):
            for trial in range(1, 4):
                unsafe = EVIDENCE / (
                    f"sandbox-harness-{operation}-ambiguous-effect-unsafe-trial-{trial}"
                )
                protected = EVIDENCE / (
                    f"sandbox-harness-{operation}-ambiguous-effect-protected-trial-{trial}"
                )
                unsafe_verdict = read_json(unsafe / "verdict.json")
                protected_verdict = read_json(protected / "verdict.json")
                unsafe_destination = read_json(unsafe / "destination-state.json")
                protected_destination = read_json(
                    protected / "destination-state.json"
                )
                unsafe_input = read_json(unsafe / "effective-input.json")
                protected_input = read_json(protected / "effective-input.json")

                self.assertEqual(unsafe_verdict["class"], "valid-fail")
                self.assertIn(
                    "duplicate_physical_effect", unsafe_verdict["reason_codes"]
                )
                self.assertEqual(protected_verdict["class"], "valid-pass")
                self.assertEqual(
                    [attempt["applied"] for attempt in unsafe_destination["attempts"]],
                    [True, True],
                )
                self.assertEqual(
                    [
                        attempt["applied"]
                        for attempt in protected_destination["attempts"]
                    ],
                    [True, False],
                )
                self.assertEqual(
                    unsafe_input["settings"]["fault_boundary"],
                    f"provider-{operation}-effect-committed",
                )
                self.assertEqual(
                    protected_input["settings"]["fault_boundary"],
                    f"provider-{operation}-effect-committed",
                )

    def test_protected_receipt_replay_keeps_one_operation_and_one_receipt(
        self,
    ) -> None:
        for operation in ("start", "command", "snapshot", "stop"):
            trial = EVIDENCE / (
                f"sandbox-harness-{operation}-ambiguous-effect-protected-trial-1"
            )
            provider = read_json(trial / "provider-state.json")
            attempts = [
                attempt
                for attempt in provider["attempts"]
                if attempt["kind"] == operation
            ]

            self.assertEqual(len(attempts), 2)
            self.assertEqual(attempts[0]["operation_id"], attempts[1]["operation_id"])
            self.assertEqual(
                attempts[0]["result"]["receipt_id"],
                attempts[1]["result"]["receipt_id"],
            )
            self.assertEqual([attempt["applied"] for attempt in attempts], [True, False])
            self.assertEqual(attempts[1]["duplicate_of"], attempts[0]["physical_attempt_id"])

    def test_stale_writer_is_rejected_by_provider_generation_fence(self) -> None:
        for trial_number in range(1, 4):
            trial = EVIDENCE / (
                f"sandbox-harness-attached-writer-protected-trial-{trial_number}"
            )
            provider = read_json(trial / "provider-state.json")
            stale = [
                attempt
                for attempt in provider["attempts"]
                if attempt.get("logical_effect_id") == "stale-attached"
            ]

            self.assertEqual(provider["mode"], "fenced")
            self.assertEqual(provider["authority"]["generation"], 2)
            self.assertEqual(len(stale), 1)
            self.assertEqual(stale[0]["generation"], 1)
            self.assertFalse(stale[0]["applied"])
            self.assertEqual(stale[0]["rejection"], "stale_authority")
            self.assertNotIn("stale-attached", provider["instances"][0]["effects"])

    def test_unsafe_stale_writer_is_independently_observed_as_applied(self) -> None:
        for trial_number in range(1, 4):
            trial = EVIDENCE / (
                f"sandbox-harness-attached-writer-unsafe-trial-{trial_number}"
            )
            verdict = read_json(trial / "verdict.json")
            provider = read_json(trial / "provider-state.json")
            stale = [
                attempt
                for attempt in provider["attempts"]
                if attempt.get("logical_effect_id") == "stale-attached"
            ]

            self.assertEqual(verdict["class"], "valid-fail")
            self.assertIn("stale_action_accepted", verdict["reason_codes"])
            self.assertEqual(provider["mode"], "unsafe")
            self.assertEqual(len(stale), 2)
            self.assertTrue(all(attempt["applied"] for attempt in stale))
            self.assertIn("stale-attached", provider["instances"][0]["effects"])

    def test_snapshot_fork_restores_exact_declared_workspace_prefix(self) -> None:
        for probe in ("unsafe", "protected"):
            for trial_number in range(1, 4):
                trial = EVIDENCE / (
                    "sandbox-harness-snapshot-ambiguous-effect-"
                    f"{probe}-trial-{trial_number}"
                )
                provider = read_json(trial / "provider-state.json")
                fork = next(
                    instance
                    for instance in provider["instances"]
                    if instance.get("parent_snapshot_id")
                )
                snapshot = next(
                    item
                    for item in provider["snapshots"]
                    if item["snapshot_id"] == fork["parent_snapshot_id"]
                )

                self.assertEqual(fork["effects"], snapshot["effects"])
                self.assertEqual(fork["workspace_sha256"], snapshot["workspace_sha256"])
                self.assertEqual(fork["effects"], ["snapshot-prefix"])

    def test_parent_close_reconciliation_uses_provider_state_as_oracle(self) -> None:
        for trial_number in range(1, 4):
            unsafe = EVIDENCE / (
                f"sandbox-harness-parent-close-unsafe-trial-{trial_number}"
            )
            protected = EVIDENCE / (
                f"sandbox-harness-parent-close-protected-trial-{trial_number}"
            )
            unsafe_verdict = read_json(unsafe / "verdict.json")
            protected_verdict = read_json(protected / "verdict.json")
            protected_final = read_json(protected / "provider-state-final.json")

            self.assertEqual(unsafe_verdict["class"], "valid-fail")
            self.assertEqual(unsafe_verdict["active_instances_after_recovery"], 1)
            self.assertEqual(protected_verdict["class"], "valid-pass")
            self.assertEqual(protected_verdict["active_instances_at_close"], 1)
            self.assertEqual(protected_verdict["active_instances_after_recovery"], 0)
            self.assertTrue(protected_verdict["cleanup_receipt"])
            reconcile_stops = [
                attempt
                for attempt in protected_final["attempts"]
                if attempt["kind"] == "stop"
                and attempt["operation_id"].startswith("reconcile/")
            ]
            self.assertEqual(len(reconcile_stops), 1)
            self.assertEqual(
                reconcile_stops[0]["result"]["receipt_id"],
                protected_verdict["cleanup_receipt"],
            )
            self.assertFalse(any(item["active"] for item in protected_final["instances"]))

    def test_every_trial_preserves_a_provider_journal_and_temporal_history(self) -> None:
        files, tree_digest = regular_files_and_digest(EVIDENCE)
        self.assertEqual(len(files), TREE_FILE_COUNT)
        self.assertEqual(tree_digest, TREE_SHA256)
        trials = sorted(path for path in EVIDENCE.iterdir() if path.is_dir())

        self.assertEqual(len(trials), 36)
        for trial in trials:
            self.assertFalse(trial.is_symlink(), trial)
            self.assertEqual(trial.resolve().parent, EVIDENCE.resolve())
            if "parent-close" in trial.name:
                self.assertTrue((trial / "provider-state-at-close.json").is_file())
                self.assertTrue((trial / "provider-state-final.json").is_file())
                self.assertTrue((trial / "parent-history.json").is_file())
                self.assertTrue((trial / "child-history.json").is_file())
                continue
            native = read_json(trial / "native-history-or-journal-export.json")
            kinds = {item["kind"] for item in native}
            self.assertIn("provider_journal", kinds)
            self.assertIn("temporal_history", kinds)

    def test_cited_artifact_hashes_and_source_pins_match_bytes_on_disk(self) -> None:
        trials = sorted(path for path in EVIDENCE.iterdir() if path.is_dir())

        for trial in trials:
            manifest = read_json(trial / "manifest.json")
            if "parent-close" in trial.name:
                self.assertEqual(manifest["source_sha256"], SOURCE_SHA256)
                self.assertEqual(manifest["upstream_commit"], UPSTREAM_COMMIT)
                continue

            for relative_path, expected_sha256 in manifest["evidence_sha256"].items():
                artifact = confined_artifact(trial, relative_path)
                actual_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()
                self.assertEqual(actual_sha256, expected_sha256, artifact)

            effective_input = read_json(trial / "effective-input.json")
            self.assertEqual(effective_input["adapter_version"], SOURCE_SHA256)
            self.assertEqual(effective_input["agent_binary_sha256"], SOURCE_SHA256)
            self.assertEqual(
                effective_input["settings"]["upstream_commit"], UPSTREAM_COMMIT
            )

    def test_runner_exposes_read_only_audit_and_fresh_checkout_paths(self) -> None:
        result = subprocess.run(
            [str(RUNNER), "--help"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("audit", result.stdout)
        self.assertIn("critical", result.stdout)
        self.assertIn("all", result.stdout)
        readme = README.read_text(encoding="utf-8")
        self.assertIn("./cookbooks/coding-agents/05-sandbox-lifecycle/run.sh all", readme)

    def test_critical_path_runs_provider_contract_and_history_replay(self) -> None:
        runner = RUNNER.read_text(encoding="utf-8")

        self.assertIn("TestStoreDistinguishesUnsafeAndIdempotentCommandDelivery", runner)
        self.assertIn("TestStoreRejectsStaleAttachedWriterInFencedMode", runner)
        self.assertIn("TestStoreRestoresExactSnapshotPrefix", runner)
        self.assertIn("TestReconcileActiveInstancesAndExclusiveWrites", runner)
        self.assertIn("TestCurrentWorkflowsReplayCapturedParentCloseHistories", runner)


if __name__ == "__main__":
    unittest.main()
