from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


SUITE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SUITE_ROOT.parents[1]
README = SUITE_ROOT / "README.md"
RUNNER = SUITE_ROOT / "run-all.sh"
COOKBOOKS = (
    "01-native-agent-loop",
    "02-effect-safe-tools",
    "03-external-cli-ownership",
    "04-cancellation-and-cleanup",
    "05-sandbox-lifecycle",
    "06-bounded-recovery",
)


class CookbookSuiteTests(unittest.TestCase):
    def test_index_links_exactly_six_cookbooks_and_product_contract(self) -> None:
        text = README.read_text(encoding="utf-8")
        for cookbook in COOKBOOKS:
            self.assertTrue((SUITE_ROOT / cookbook / "README.md").is_file())
            self.assertIn(cookbook, text)
        for reference in (
            "../../docs/product/coding-agent-durability-v1.md",
            "../../specs/coding-agent-durability/v1/README.md",
            "../../contrib/codingagent/go",
            "../../contrib/codingagent/python",
            "../../benchmarks/agent-durability/conformance",
        ):
            self.assertIn(reference, text)

    def test_index_preserves_maturity_and_responsibility_boundaries(self) -> None:
        text = README.read_text(encoding="utf-8")
        for term in (
            "Temporal",
            "application",
            "destination",
            "experimental",
            "no exactly-once",
            "Normative for tested single-host Claude/Codex CLI boundaries",
            "provider or version compatibility promise",
            "mechanism conformance",
            "not a performance ranking",
        ):
            self.assertIn(term, text)

    def test_check_mode_is_executable_and_cwd_independent(self) -> None:
        if os.environ.get("COOKBOOK_SUITE_CHECK_MODE") == "1":
            self.skipTest("already executing through run-all.sh check")
        self.assertTrue(RUNNER.is_file())
        self.assertTrue(os.access(RUNNER, os.X_OK))
        completed = subprocess.run(
            [str(RUNNER), "check"],
            cwd=Path("/"),
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
