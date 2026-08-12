package lab

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestAuditMetadataRequiresPinnedAuthenticationAndHashes(t *testing.T) {
	metadata := validTestExperimentMetadata(true)
	if err := validateExperimentMetadata(metadata); err != nil {
		t.Fatalf("valid hermetic metadata: %v", err)
	}
	metadata.CodexBinarySHA256 = "not-a-hash"
	if err := validateExperimentMetadata(metadata); err == nil {
		t.Fatal("invalid source hash was accepted")
	}
	metadata = validTestExperimentMetadata(true)
	metadata.EffectBinaryPath = ""
	if err := validateExperimentMetadata(metadata); err == nil {
		t.Fatal("missing controlled-effect path was accepted")
	}
	metadata = validTestExperimentMetadata(true)
	metadata.LauncherBinaryPath = ""
	if err := validateExperimentMetadata(metadata); err == nil {
		t.Fatal("missing launcher path was accepted")
	}
	metadata = validTestExperimentMetadata(true)
	metadata.OutputSchemaPath = ""
	if err := validateExperimentMetadata(metadata); err == nil {
		t.Fatal("missing output-schema path was accepted")
	}
	metadata = validTestExperimentMetadata(false)
	if err := validateExperimentMetadata(metadata); err != nil {
		t.Fatalf("valid authenticated metadata: %v", err)
	}
	metadata.Authentication = "not-applicable-hermetic"
	if err := validateExperimentMetadata(metadata); err == nil {
		t.Fatal("authenticated evidence with hermetic provenance was accepted")
	}
}

func TestAuditRequiresPinnedSandboxOnEveryRecordedProcess(t *testing.T) {
	metadata := validTestExperimentMetadata(false)
	initial := ProcessRecord{Binary: "/opt/codex", WorkDir: "/fixture", Args: []string{
		"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
		"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
		"--sandbox", "workspace-write", "--output-schema", "/schema.json", "-",
	}}
	if err := validatePinnedCodexProcess(initial, metadata, "/opt/codex", ""); err != nil {
		t.Fatalf("valid initial process: %v", err)
	}
	resumed := ProcessRecord{Binary: "/opt/codex", WorkDir: "/fixture", Args: []string{
		"--cd", "/fixture", "exec", "--sandbox", "workspace-write", "resume",
		"--json", "--ignore-user-config", "--ignore-rules", "--model", "gpt-5.6-sol",
		"-c", `model_reasoning_effort="low"`, "--output-schema", "/schema.json",
		"0199a213-81c0-7800-8aa1-bbab2a035a53", "-",
	}}
	if err := validatePinnedCodexProcess(resumed, metadata, "/opt/codex", "0199a213-81c0-7800-8aa1-bbab2a035a53"); err != nil {
		t.Fatalf("valid resumed process: %v", err)
	}
	for name, args := range map[string][]string{
		"missing-sandbox": {
			"--cd", "/fixture", "exec", "resume", "--json", "--ignore-user-config", "--ignore-rules",
			"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
			"--output-schema", "/schema.json", "0199a213-81c0-7800-8aa1-bbab2a035a53", "-",
		},
		"sandbox-after-resume": {
			"--cd", "/fixture", "exec", "resume", "--sandbox", "workspace-write", "--json",
			"--ignore-user-config", "--ignore-rules", "--model", "gpt-5.6-sol", "-c",
			`model_reasoning_effort="low"`, "--output-schema", "/schema.json",
			"0199a213-81c0-7800-8aa1-bbab2a035a53", "-",
		},
		"wrong-model": {
			"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
			"--model", "other", "-c", `model_reasoning_effort="low"`,
			"--sandbox", "workspace-write", "--output-schema", "/schema.json", "-",
		},
		"bypass": {
			"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
			"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
			"--sandbox", "workspace-write", "--dangerously-bypass-approvals-and-sandbox",
			"--output-schema", "/schema.json", "-",
		},
		"alternate-schema": {
			"--cd", "/fixture", "exec", "--json", "--ignore-user-config", "--ignore-rules",
			"--model", "gpt-5.6-sol", "-c", `model_reasoning_effort="low"`,
			"--sandbox", "workspace-write", "--output-schema", "/other.json", "-",
		},
	} {
		t.Run(name, func(t *testing.T) {
			process := initial
			process.Args = args
			if err := validatePinnedCodexProcess(process, metadata, "/opt/codex", ""); err == nil {
				t.Fatal("unsafe or mismatched recorded process was accepted")
			}
		})
	}
	wrongBinary := initial
	wrongBinary.Binary = "/opt/other"
	if err := validatePinnedCodexProcess(wrongBinary, metadata, "/opt/codex", ""); err == nil {
		t.Fatal("alternate recorded executable was accepted")
	}
	if err := validatePinnedCodexProcess(resumed, metadata, "/opt/codex", "0199a213-81c0-7800-8aa1-bbab2a035a54"); err == nil {
		t.Fatal("resume argv thread was not bound to its receipt")
	}
}

func TestAuditWorkspaceDestinationRequiresExactPhysicalIdentity(t *testing.T) {
	appliedAt := time.Now().UTC()
	workspace := []WorkspaceEffect{{
		LogicalEffectID: "effect-1", PhysicalAttemptID: "attempt-1", Payload: "controlled-edit",
		ActorID: "actor-1", ProcessIdentity: "pid:1:start:test", AppliedAt: appliedAt,
	}}
	destination := []EffectAttempt{{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		PhysicalAttemptID: "attempt-1", ActorID: "actor-1", ProcessIdentity: "pid:1:start:test",
		Applied: true, AppliedAt: appliedAt,
	}}
	if err := validateWorkspaceDestination(workspace, destination); err != nil {
		t.Fatalf("matching receipts: %v", err)
	}
	if err := validateWorkspaceDestination(workspace, nil); err == nil {
		t.Fatal("mismatched receipt count was accepted")
	}
	destination[0].ProcessIdentity = "pid:2:start:other"
	if err := validateWorkspaceDestination(workspace, destination); err == nil {
		t.Fatal("mismatched process identity was accepted")
	}
}

func TestAuditPopulationRequiresExactScheduleAndRootEntries(t *testing.T) {
	root := t.TempDir()
	names := []string{"suite-summary.json", "temporal-server.log", "temporal.db"}
	seen := make(map[string]bool)
	seenSchedule := make(map[string]bool)
	var runs []string
	for _, boundary := range experimentSchedule(RecoveryModeUnsafeFresh) {
		name := "codex-direct-unsafe-fresh-" + string(boundary) + "-trial-1"
		if err := os.Mkdir(filepath.Join(root, name), 0o750); err != nil {
			t.Fatal(err)
		}
		seen[name] = true
		seenSchedule["1/"+string(boundary)] = true
		runs = append(runs, filepath.Join(root, name))
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	report := EvidenceAudit{Mode: RecoveryModeUnsafeFresh}
	suite := ExperimentResult{RunDirectories: runs}
	if err := validateEvidencePopulation(report, suite, entries, seen, seenSchedule); err != nil {
		t.Fatalf("valid population: %v", err)
	}
	delete(seenSchedule, "1/"+string(FaultAfterFinalOutput))
	if err := validateEvidencePopulation(report, suite, entries, seen, seenSchedule); err == nil {
		t.Fatal("incomplete exact schedule was accepted")
	}
	seenSchedule["1/"+string(FaultAfterFinalOutput)] = true
	if err := os.WriteFile(filepath.Join(root, "unexpected"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEvidencePopulation(report, suite, entries, seen, seenSchedule); err == nil {
		t.Fatal("unexpected root artifact was accepted")
	}
}

func TestAuditPopulationRejectsNonRegularSuiteArtifacts(t *testing.T) {
	root := t.TempDir()
	seen := make(map[string]bool)
	seenSchedule := make(map[string]bool)
	var runs []string
	for _, boundary := range experimentSchedule(RecoveryModeUnsafeFresh) {
		name := "codex-direct-unsafe-fresh-" + string(boundary) + "-trial-1"
		if err := os.Mkdir(filepath.Join(root, name), 0o750); err != nil {
			t.Fatal(err)
		}
		seen[name] = true
		seenSchedule["1/"+string(boundary)] = true
		runs = append(runs, filepath.Join(root, name))
	}
	for _, name := range []string{"suite-summary.json", "temporal-server.log"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mkfifo(filepath.Join(root, "temporal.db"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEvidencePopulation(
		EvidenceAudit{Mode: RecoveryModeUnsafeFresh},
		ExperimentResult{RunDirectories: runs}, entries, seen, seenSchedule,
	); err == nil {
		t.Fatal("non-regular suite artifact was accepted")
	}
}

func TestAuditBoundedFileAndSHAValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact")
	if err := os.WriteFile(path, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := readBoundedFile(path, 2); err != nil || string(data) != "ok" {
		t.Fatalf("bounded file = %q, err=%v", data, err)
	}
	if _, err := readBoundedFile(path, 1); err == nil {
		t.Fatal("oversized file was accepted")
	}
	if _, err := readBoundedFile(filepath.Dir(path), 10); err == nil {
		t.Fatal("directory was accepted as an artifact")
	}
	if validSHA256(string(make([]byte, 64))) {
		t.Fatal("non-hexadecimal SHA-256 was accepted")
	}
	if validSHA256("abc") || !validSHA256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") {
		t.Fatal("SHA-256 length or alphabet validation failed")
	}
}

func validTestExperimentMetadata(hermetic bool) experimentMetadata {
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	authentication := "wrapper-and-pinned-cli-profile-logged-in-using-chatgpt"
	if hermetic {
		authentication = "not-applicable-hermetic"
	}
	return experimentMetadata{
		CodexVersion: "codex-cli 0.147.0", CodexBinaryPath: "/opt/codex",
		CodexBinarySHA256: hash, CodexWrapperPath: "/opt/codex-2", CodexWrapperSHA256: hash,
		CodexHomePath: "/profile", Model: "gpt-5.6-sol", ReasoningEffort: "low",
		Sandbox: "workspace-write", Hermetic: hermetic, Authentication: authentication,
		InvocationPath: "pinned-underlying-cli-with-codex-2-profile", WorkerSHA256: hash,
		EffectBinaryPath: "/opt/effect",
		EffectSHA256:     hash, LauncherBinaryPath: "/opt/launcher", LauncherSHA256: hash,
		OutputSchemaPath: "/schema.json", SchemaSHA256: hash, HarnessSHA256: hash,
	}
}
