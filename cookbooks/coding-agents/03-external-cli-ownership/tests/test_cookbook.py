from __future__ import annotations

import hashlib
import json
import os
import subprocess
import unittest
from pathlib import Path


COOKBOOK_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = COOKBOOK_ROOT.parents[2]
README = COOKBOOK_ROOT / "README.md"
RUNNER = COOKBOOK_ROOT / "run.sh"
EXPERIMENT = REPOSITORY_ROOT / "experiments/durable-vendor-sessions/claude-direct"


def evidence_root(environment_name: str, default_relative: str) -> Path:
    configured = os.environ.get(environment_name)
    if configured:
        return Path(configured)
    return EXPERIMENT / default_relative


DIRECT = evidence_root(
    "COOKBOOK_DIRECT_ROOT", "evidence/claude-direct-20260808-v5"
)
RESUME = evidence_root(
    "COOKBOOK_RESUME_ROOT", "evidence/claude-direct-resume-20260810-v5"
)
FENCED = evidence_root(
    "COOKBOOK_FENCED_ROOT", "evidence/claude-direct-fenced-hermetic-20260811-v4"
)
HASHED_ARTIFACTS = {
    "authority-state.json",
    "common-events.jsonl",
    "destination-state.json",
    "effective-input.json",
    "fault-boundary.json",
    "native-history-or-journal-export.json",
    "process-observations.json",
}
TRANSPORT_INDEX_DIGESTS = {
    "evidence-transport": "46a82476b4b47b103732121a10157434caefbc4661b2f7cc02cdce7df1714514",
    "resume-evidence-transport": "107da44f12f0e9c9b6bd0a76095790e2943dd655c141d74d48ebfff779f838d3",
    "fenced-evidence-transport-v2": "d43a5463f0dcfd852744cbf52ca649f4898873985ea61a516c1438ce18f40c02",
}


def load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} is not a JSON object")
    return value


def confined_artifact(run: Path, relative: str) -> Path:
    if not relative or "/" in relative or "\\" in relative or relative in {".", ".."}:
        raise ValueError(f"unconfined artifact path: {relative!r}")
    artifact = run / relative
    if artifact.is_symlink() or not artifact.is_file():
        raise ValueError(f"artifact is not a regular non-symlink file: {artifact}")
    if artifact.resolve().parent != run.resolve():
        raise ValueError(f"artifact escapes run root: {artifact}")
    return artifact


class CookbookContractTests(unittest.TestCase):
    def test_readme_states_complete_contract_and_maturity(self) -> None:
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
        for term in (
            "Normative",
            "direct relaunch",
            "resume-only",
            "start-or-attach",
            "generation",
            "capability",
            "fence-before-replace",
            "authenticated Claude",
            "Codex",
            "102 replayed histories",
            "not exactly once",
        ):
            self.assertIn(term, text)

    def test_citations_and_runner_resolve(self) -> None:
        text = README.read_text(encoding="utf-8")
        for finding in (
            "0010-direct-claude-activity-retry-duplicates-turns-and-effects",
            "0019-claude-resume-preserves-session-identity-not-effect-safety",
            "0020-application-fenced-claude-supervisor-survives-worker-loss",
            "0021-codex-thread-resume-is-not-turn-authority",
        ):
            self.assertTrue((REPOSITORY_ROOT / f"docs/findings/{finding}.md").is_file())
            self.assertIn(finding, text)
        self.assertTrue(RUNNER.is_file())
        self.assertTrue(os.access(RUNNER, os.X_OK))
        runner = RUNNER.read_text(encoding="utf-8")
        self.assertIn("CODEX_DIRECT_TRANSPORT_AUDIT=1", runner)
        self.assertIn("TestAdmittedTransportsReconstructEveryVerdict", runner)

    def test_three_populations_are_sealed_and_distinguishing(self) -> None:
        expected = ((DIRECT, 12, 3, 9), (RESUME, 12, 3, 9), (FENCED, 15, 15, 0))
        for root, count, passes, failures in expected:
            runs = sorted(path for path in root.iterdir() if path.is_dir())
            self.assertEqual(len(runs), count, root)
            observed_passes = 0
            observed_failures = 0
            for run in runs:
                manifest = load_json(run / "manifest.json")
                verdict = load_json(run / "verdict.json")
                hashes = manifest["evidence_sha256"]
                self.assertIsInstance(hashes, dict)
                self.assertEqual(set(hashes), HASHED_ARTIFACTS)
                for relative, expected_digest in hashes.items():
                    artifact = confined_artifact(run, relative)
                    self.assertEqual(
                        hashlib.sha256(artifact.read_bytes()).hexdigest(),
                        expected_digest,
                        artifact,
                    )
                if verdict["class"] == "valid-pass":
                    observed_passes += 1
                elif verdict["class"] == "valid-fail":
                    observed_failures += 1
                    self.assertIn(
                        "duplicate_physical_effect", verdict["reason_codes"]
                    )
                else:
                    self.fail(f"unexpected verdict in {run}: {verdict}")
            self.assertEqual((observed_passes, observed_failures), (passes, failures))

    def test_clone_safe_transport_indexes_match_pinned_digests(self) -> None:
        for directory, expected_digest in TRANSPORT_INDEX_DIGESTS.items():
            index = EXPERIMENT / directory / "transport-index.json"
            self.assertTrue(index.is_file(), index)
            self.assertFalse(index.is_symlink(), index)
            self.assertEqual(hashlib.sha256(index.read_bytes()).hexdigest(), expected_digest)

    def test_fenced_population_has_one_effect_and_twelve_attachments(self) -> None:
        kinds: list[str] = []
        for run in sorted(path for path in FENCED.iterdir() if path.is_dir()):
            verdict = load_json(run / "verdict.json")
            metrics = verdict["metrics"]
            self.assertEqual(metrics["physical_effect_count"], 1)
            self.assertEqual(metrics["accepted_outcome_count"], 1)
            self.assertEqual(metrics["stale_action_accept_count"], 0)
            for line in (run / "common-events.jsonl").read_text(encoding="utf-8").splitlines():
                kinds.append(json.loads(line)["kind"])
        self.assertEqual(kinds.count("executor_attached"), 12)
        self.assertEqual(kinds.count("effect_accepted"), 15)
        self.assertEqual(kinds.count("outcome_accepted"), 15)

    def test_check_mode_is_cwd_independent(self) -> None:
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
