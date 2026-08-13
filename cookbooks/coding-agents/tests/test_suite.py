from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SUITE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SUITE_ROOT.parents[1]
README = SUITE_ROOT / "README.md"
RUNNER = SUITE_ROOT / "run-all.sh"
QUICKSTART = SUITE_ROOT / "quickstart.sh"
QUICKSTART_README = SUITE_ROOT / "quickstart/README.md"
PRODUCT_BRIEF = REPOSITORY_ROOT / "docs/product/fault-tested-coding-agent-cookbooks.md"
PRESENTATION_README = SUITE_ROOT / "presentation/README.md"
REPOSITORY_README = REPOSITORY_ROOT / "README.md"
DEVCONTAINER_ROOT = REPOSITORY_ROOT / ".devcontainer"
DEVCONTAINER = DEVCONTAINER_ROOT / "devcontainer.json"
DEVCONTAINER_DOCKERFILE = DEVCONTAINER_ROOT / "Dockerfile"
DEVCONTAINER_SETUP = DEVCONTAINER_ROOT / "post-create.sh"
DEVCONTAINER_README = DEVCONTAINER_ROOT / "README.md"
DEV_SMOKE = SUITE_ROOT / "dev-smoke.sh"
EXPLORER = SUITE_ROOT / "explore.sh"
EXPLORER_README = SUITE_ROOT / "explorer/README.md"
TUTORIALS = SUITE_ROOT / "tutorials/README.md"
CODE_EXCHANGE_PREVIEW = REPOSITORY_ROOT / "docs/product/code-exchange-submission.json"
CODE_EXCHANGE_README = REPOSITORY_ROOT / "docs/product/code-exchange-submission.md"
PRODUCT_WORKFLOW = REPOSITORY_ROOT / ".github/workflows/coding-agent-product.yml"
LICENSE = REPOSITORY_ROOT / "LICENSE"
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

    def test_public_information_architecture_is_explicit(self) -> None:
        text = README.read_text(encoding="utf-8")
        for title in (
            "## Start",
            "## Patterns",
            "## Scenarios",
            "## Evidence",
            "## Protocol",
            "## Research",
        ):
            self.assertIn(title, text)
        self.assertIn("presentation/README.md", text)
        self.assertIn("../../docs/product/fault-tested-coding-agent-cookbooks.md", text)

    def test_product_brief_positions_the_surface_without_weakening_evidence(self) -> None:
        self.assertTrue(PRODUCT_BRIEF.is_file())
        text = PRODUCT_BRIEF.read_text(encoding="utf-8")
        for term in (
            "Fault-Tested Durability Patterns for Coding Agents",
            "first trustworthy recovery",
            "exact barrier",
            "unsafe",
            "protected",
            "native history",
            "responsibility",
            "falsifier",
            "not an oracle",
            "generic runtime parity",
            "exactly once",
            "controlled-compute performance",
            "provider compatibility",
        ):
            self.assertIn(term, text)

    def test_presentation_contract_and_root_readme_point_to_the_product_path(self) -> None:
        self.assertTrue(PRESENTATION_README.is_file())
        presentation = PRESENTATION_README.read_text(encoding="utf-8")
        for term in (
            "read-only",
            "non-authoritative",
            "verified evidence",
            "native history",
            "correction lineage",
        ):
            self.assertIn(term, presentation)
        root = REPOSITORY_README.read_text(encoding="utf-8")
        self.assertIn("Fault-Tested Durability Patterns for Coding Agents", root)
        self.assertIn("cookbooks/coding-agents/README.md", root)

    def test_quickstart_is_unfilterable_and_requires_exact_audit_receipts(self) -> None:
        self.assertTrue(QUICKSTART.is_file())
        self.assertTrue(os.access(QUICKSTART, os.X_OK))
        text = QUICKSTART.read_text(encoding="utf-8")
        for term in (
            "CODEX_DIRECT_TRANSPORT_AUDIT=1",
            "test -race -count=1 -json",
            "-timeout=2m",
            "^TestAdmittedTransportsReconstructEveryVerdict$",
            "./experiments/durable-vendor-sessions/codex-direct/internal/lab",
            "./cookbooks/coding-agents/quickstart/cmd/summary",
            "if (( $# != 0 ))",
        ):
            self.assertIn(term, text)
        completed = subprocess.run(
            ["bash", "-n", str(QUICKSTART)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_quickstart_docs_preserve_the_evidence_boundary(self) -> None:
        self.assertTrue(QUICKSTART_README.is_file())
        text = QUICKSTART_README.read_text(encoding="utf-8")
        for term in (
            "## Question",
            "## Invariant",
            "## Failure boundary",
            "## Oracle",
            "## Run",
            "## Evidence",
            "## Responsibility split",
            "## Falsifier",
            "not new evidence",
            "102",
        ):
            self.assertIn(term, text)
        self.assertIn("./cookbooks/coding-agents/quickstart.sh", README.read_text(encoding="utf-8"))

    def test_devcontainer_is_pinned_non_root_and_resource_bounded(self) -> None:
        config = json.loads(DEVCONTAINER.read_text(encoding="utf-8"))
        self.assertEqual(config["build"], {"dockerfile": "Dockerfile", "context": "."})
        self.assertEqual(config["remoteUser"], "vscode")
        self.assertEqual(config["containerUser"], "vscode")
        self.assertTrue(config["init"])
        self.assertEqual(config["shutdownAction"], "stopContainer")
        self.assertEqual(
            config["hostRequirements"],
            {"cpus": 2, "memory": "4gb", "storage": "16gb"},
        )
        self.assertEqual(config["postCreateCommand"], "bash .devcontainer/post-create.sh")
        self.assertEqual(config["forwardPorts"], [8080])
        self.assertEqual(
            config["portsAttributes"]["8080"],
            {"label": "Recovery evidence explorer", "onAutoForward": "notify"},
        )
        self.assertIn("--cap-drop=ALL", config["runArgs"])
        self.assertIn("--security-opt=no-new-privileges:true", config["runArgs"])
        serialized = json.dumps(config)
        for forbidden in ("localEnv:", "API_KEY", "TOKEN", "PASSWORD", "docker.sock"):
            self.assertNotIn(forbidden, serialized)

    def test_devcontainer_toolchains_are_exact_and_setup_is_bounded(self) -> None:
        dockerfile = DEVCONTAINER_DOCKERFILE.read_text(encoding="utf-8")
        for exact in (
            "docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32",
            "golang:1.25.12-bookworm@sha256:6359592445455f2dbe2412bed411336035bc019a50017720d77454ffdd6d0f82",
            "ghcr.io/astral-sh/uv:0.11.2@sha256:c4f5de312ee66d46810635ffc5df34a1973ba753e7241ce3a08ef979ddd7bea5",
            "uv python install 3.12.12",
            "USER vscode",
        ):
            self.assertIn(exact, dockerfile)
        self.assertNotIn(":latest", dockerfile)
        self.assertNotIn("curl ", dockerfile)

        self.assertTrue(os.access(DEVCONTAINER_SETUP, os.X_OK))
        setup = DEVCONTAINER_SETUP.read_text(encoding="utf-8")
        for exact in ("go1.25.12", "0.11.2", "3.12.12", "go mod download"):
            self.assertIn(exact, setup)
        self.assertIn("env -i", setup)
        self.assertIn("GOPROXY=https://proxy.golang.org", setup)
        self.assertNotIn('"${SCRIPT_DIR}/../cookbooks/coding-agents/quickstart.sh"', setup)

    def test_dev_smoke_is_cwd_independent_credential_free_and_ci_shaped(self) -> None:
        self.assertTrue(os.access(DEV_SMOKE, os.X_OK))
        text = DEV_SMOKE.read_text(encoding="utf-8")
        for exact in (
            "quickstart.sh",
            "COOKBOOK_SUITE_CHECK_MODE=1",
            "python3.12 -m unittest",
            "go test -race -count=1",
            "./cookbooks/coding-agents/presentation/...",
            "./cookbooks/coding-agents/quickstart/...",
            "if (( $# != 0 ))",
        ):
            self.assertIn(exact, text)
        self.assertIn("env -i", QUICKSTART.read_text(encoding="utf-8"))
        for script in (DEVCONTAINER_SETUP, DEV_SMOKE):
            completed = subprocess.run(
                ["bash", "-n", str(script)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_explorer_launcher_is_fixed_loopback_and_credential_free(self) -> None:
        self.assertTrue(EXPLORER.is_file())
        self.assertTrue(os.access(EXPLORER, os.X_OK))
        text = EXPLORER.read_text(encoding="utf-8")
        for exact in (
            "if (( $# != 0 ))",
            "env -i",
            "--listen 127.0.0.1:8080",
            "./cookbooks/coding-agents/explorer/cmd/explorer",
        ):
            self.assertIn(exact, text)
        for forbidden in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "--listen \"$", "localhost:8080"):
            self.assertNotIn(forbidden, text)
        completed = subprocess.run(
            ["bash", "-n", str(EXPLORER)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_explorer_docs_preserve_the_evidence_boundary(self) -> None:
        text = EXPLORER_README.read_text(encoding="utf-8")
        for term in (
            "## Question",
            "## Invariant",
            "## Failure boundary",
            "## Oracle",
            "## Run",
            "## Evidence",
            "## Responsibility split",
            "## Falsifier",
            "loopback",
            "read-only",
            "not the oracle",
            "exactly-once",
        ):
            self.assertIn(term, text)
        self.assertIn("explore.sh", README.read_text(encoding="utf-8"))

    def test_tutorial_path_teaches_failure_before_mechanism(self) -> None:
        text = TUTORIALS.read_text(encoding="utf-8")
        for term in (
            "First trustworthy recovery",
            "Unsafe",
            "Protected",
            "stable identity",
            "authority",
            "destination",
            "native history",
            "responsibility",
            "falsifier",
            "./cookbooks/coding-agents/quickstart.sh",
            "./cookbooks/coding-agents/explore.sh",
            "./cookbooks/coding-agents/run-all.sh check",
            "./cookbooks/coding-agents/dev-smoke.sh",
        ):
            self.assertIn(term, text)
        self.assertIn("tutorials/README.md", README.read_text(encoding="utf-8"))

    def test_code_exchange_preview_matches_the_current_submission_contract(self) -> None:
        preview = json.loads(CODE_EXCHANGE_PREVIEW.read_text(encoding="utf-8"))
        self.assertEqual(preview["project_link"], "https://github.com/sjarmak/agent-durability-lab")
        self.assertEqual(preview["languages"], ["Go", "Python"])
        self.assertLessEqual(len(preview["short_description"]), 256)
        self.assertTrue(preview["long_description"].strip())
        self.assertTrue(preview["authors"])
        self.assertEqual(preview["submission_status"], "preview-only")
        self.assertEqual(preview["verified_at"], "2026-08-13")
        self.assertIn("temporal-community/code-exchange", preview["source_template"])
        self.assertTrue(LICENSE.read_text(encoding="utf-8").startswith("MIT License"))
        rendered = CODE_EXCHANGE_README.read_text(encoding="utf-8")
        for value in (preview["project_link"], preview["short_description"], preview["authors"][0]):
            self.assertIn(value, rendered)
        self.assertIn("No submission has been made", rendered)

    def test_public_tutorial_commands_are_exercised_by_the_pinned_product_workflow(self) -> None:
        workflow = PRODUCT_WORKFLOW.read_text(encoding="utf-8")
        for exact in (
            "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
            "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
            "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",
            "go-version: 1.25.12",
            "python-version: '3.12'",
            "./cookbooks/coding-agents/dev-smoke.sh",
            "./cookbooks/coding-agents/run-all.sh check",
        ):
            self.assertIn(exact, workflow)
        smoke = DEV_SMOKE.read_text(encoding="utf-8")
        self.assertIn('"${SCRIPT_DIR}/quickstart.sh"', smoke)
        self.assertIn('"${SCRIPT_DIR}/explore.sh"', smoke)

    def test_quickstart_does_not_pass_provider_credentials_to_go_children(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            capture = temporary_path / "environment.txt"
            fake_go = temporary_path / "go"
            fake_go.write_text(
                "#!/bin/bash\n"
                f"printf '%s|%s|%s\\n' \"$1\" \"${{OPENAI_API_KEY-unset}}\" "
                f"\"${{ANTHROPIC_API_KEY-unset}}\" >> {capture!s}\n"
                "if [[ \"$1\" == env ]]; then\n"
                "  printf '%s\\n' /tmp/coding-agent-empty-modcache\n"
                "  exit 0\n"
                "fi\n"
                "exit 91\n",
                encoding="utf-8",
            )
            fake_go.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{temporary}:{environment['PATH']}",
                    "OPENAI_API_KEY": "must-not-reach-child",
                    "ANTHROPIC_API_KEY": "must-not-reach-child",
                }
            )
            completed = subprocess.run(
                [str(QUICKSTART)],
                cwd=Path("/"),
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertEqual(
                capture.read_text(encoding="utf-8").splitlines(),
                ["env|unset|unset", "test|unset|unset"],
            )

    def test_devcontainer_docs_state_onboarding_and_evidence_limits(self) -> None:
        text = DEVCONTAINER_README.read_text(encoding="utf-8")
        normalized = " ".join(text.split())
        for term in (
            "Codespaces",
            "Dev Containers",
            "./cookbooks/coding-agents/dev-smoke.sh",
            "2 CPU",
            "4 GB",
            "16 GB",
            "credential-free",
            "not controlled compute",
            "not performance evidence",
            "does not generate evidence",
            "authenticated",
            "verified shutdown",
        ):
            self.assertIn(term, normalized)
        cookbook_index = README.read_text(encoding="utf-8")
        self.assertIn("../../.devcontainer/README.md", cookbook_index)
        self.assertIn("./cookbooks/coding-agents/dev-smoke.sh", REPOSITORY_README.read_text(encoding="utf-8"))

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
