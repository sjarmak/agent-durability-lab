package lab

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestInspectAuditedPopulationRelocatesLegacySuitePaths(t *testing.T) {
	root := t.TempDir()
	runNames := []string{"run-a", "run-b"}
	for _, name := range runNames {
		if err := os.Mkdir(filepath.Join(root, name), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	suite := ExperimentResult{
		EvidenceRoot: "/original/host/evidence",
		RunDirectories: []string{
			"/original/host/evidence/run-a",
			"/original/host/evidence/run-b",
		},
	}
	data, err := json.Marshal(suite)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "suite-summary.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"temporal-server.log", "temporal.db"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("sealed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	population, err := inspectAuditedPopulation(t.Context(), root, len(runNames))
	if err != nil {
		t.Fatalf("inspect relocated population: %v", err)
	}
	if population.suite.EvidenceRoot != root {
		t.Fatalf("normalized root = %q, want %q", population.suite.EvidenceRoot, root)
	}
	for index, name := range runNames {
		want := filepath.Join(root, name)
		if population.suite.RunDirectories[index] != want {
			t.Fatalf("normalized run %d = %q, want %q", index, population.suite.RunDirectories[index], want)
		}
	}
}

func TestInspectAuditedPopulationRejectsUnexpectedRootEntry(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "run-a"), 0o750); err != nil {
		t.Fatal(err)
	}
	suite := ExperimentResult{EvidenceRoot: root, RunDirectories: []string{filepath.Join(root, "run-a")}}
	data, err := json.Marshal(suite)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{
		"suite-summary.json":  data,
		"temporal-server.log": []byte("sealed"),
		"temporal.db":         []byte("sealed"),
		"unexpected":          []byte("not inventoried"),
	} {
		if err := os.WriteFile(filepath.Join(root, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := inspectAuditedPopulation(t.Context(), root, 1); err == nil {
		t.Fatal("unexpected suite-root entry returned nil error")
	}
}

func TestValidateFencedAuditTrialRequiresIndependentSafetyFacts(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	const (
		sessionID   = "logical-session"
		sessionUUID = "11111111-2222-4333-8444-555555555555"
		capability  = "opaque-capability"
		processID   = "pid:101:start:boot:7"
	)
	hash := workstore.HashToken(capability)
	summary := trialSummary{
		Probe: protocol.ProbeProtected, FaultBoundary: FaultAfterClaimBeforeExec, Trial: 1,
		RecoveryMode: RecoveryModeFenced, SelectedVendorSessionID: sessionUUID,
		WorkflowResult: ClaudeActivityResult{
			TemporalAttempt: 2, PhysicalAttemptID: "physical-1", VendorSessionID: sessionUUID,
			Result: "EFFECT_COMPLETE", ProcessIdentity: processID,
		},
		WorkspaceBeforeHash: "before", WorkspaceAfterHash: "after",
		WorkspaceEffects: []WorkspaceEffect{{
			LogicalEffectID: "effect-1", PhysicalAttemptID: "physical-1", Payload: "controlled-edit",
			ActorID: "supervisor", ProcessIdentity: "pid:102:start:boot:7", AppliedAt: now,
		}},
		Destination: DestinationSnapshot{Attempts: []EffectAttempt{{
			LogicalSessionID: sessionID, LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
			PhysicalAttemptID: "physical-1", ActorID: "supervisor", ProcessIdentity: processID,
			Applied: true, AppliedAt: now,
		}}},
		Authority: &workstore.Snapshot{
			SessionID: sessionID, Mode: workstore.ModeFenced, ActiveGeneration: 1,
			ActiveOwnerTokenHash: hash,
			Executors: []workstore.Executor{{
				Generation: 1, OwnerTokenHash: hash, WorkerID: "worker-one", Attempt: 1,
				PID: 101, ProcessStart: "boot:7", ProcessGroupID: 101,
				Status: workstore.ExecutorStatusCompleted, StartedAt: now,
			}},
			Effects: []workstore.AcceptedEffect{{
				Effect:     workstore.Effect{ID: "effect-1", Value: "controlled-edit"},
				Generation: 1, OwnerTokenHash: hash, AcceptedAt: now,
			}},
			Outcome: &workstore.Outcome{Value: "EFFECT_COMPLETE"},
			Events: []workstore.Event{{
				Sequence: 1, Time: now, Kind: "activity_reattached", SessionID: sessionID,
				Generation: 1, OwnerTokenHash: hash, WorkerID: "worker-two", Attempt: 2,
			}},
		},
		ReplayVerified: true,
	}
	manifest := protocol.Manifest{
		RunID: "run-1", Case: protocol.CaseAmbiguousEffect, Probe: protocol.ProbeProtected,
		Trial: 1, SessionID: sessionID,
	}
	input := protocol.EffectiveInput{Settings: map[string]string{
		"recovery_mode": string(RecoveryModeFenced), "selected_vendor_session_id": sessionUUID,
		"fault_boundary": string(FaultAfterClaimBeforeExec), "workspace_effect_count": "1",
		"workflow_history_replay_verified": "true", "raw_inventory_sha256": "inventory-hash",
	}}
	verdict := protocol.Verdict{
		RunID: manifest.RunID, Case: manifest.Case, Probe: manifest.Probe, Trial: manifest.Trial,
		Class:   protocol.VerdictValidPass,
		Metrics: protocol.Metrics{AcceptedOutcomeCount: 1, PhysicalEffectCount: 1, PhysicalAttemptCount: 1, ConcurrentOwnerCount: 1},
	}
	request := ControlledEffectInput{
		WorkspacePath: "/tmp/effects", SupervisorURL: "http://127.0.0.1", OwnershipGeneration: 1,
		OwnerCapability: capability, Payload: "controlled-edit", BarrierURL: "http://127.0.0.1",
		BarrierPoint: committedEffectBarrier, LogicalSessionID: sessionID, LogicalTurnID: "turn-1",
		LogicalEffectID: "effect-1", PhysicalAttemptID: "physical-1", ActorID: "supervisor",
	}
	attempt := auditedFencedAttempt{
		number: 1, request: request,
		process: ProcessRecord{
			AttemptID: "physical-1", ActorID: "supervisor", Binary: "/tmp/launcher",
			Args: []string{"--session-id", sessionUUID}, WorkDir: "/tmp/work", PID: 101,
			StartIdentity: "boot:7", ProcessGroupID: 101, Identity: processID, State: "running",
		},
		stream: ClaudeStreamResult{SessionID: sessionUUID, Result: "EFFECT_COMPLETE"},
	}

	if err := validateFencedAuditTrial(manifest, input, verdict, summary, attempt); err != nil {
		t.Fatalf("validate protected trial: %v", err)
	}

	tests := []struct {
		name   string
		change func(*trialSummary, *auditedFencedAttempt, *protocol.Verdict)
	}{
		{"missing attach", func(summary *trialSummary, _ *auditedFencedAttempt, _ *protocol.Verdict) {
			summary.Authority.Events = nil
		}},
		{"duplicate workspace effect", func(summary *trialSummary, _ *auditedFencedAttempt, _ *protocol.Verdict) {
			summary.WorkspaceEffects = append(summary.WorkspaceEffects, summary.WorkspaceEffects[0])
		}},
		{"capability mismatch", func(_ *trialSummary, attempt *auditedFencedAttempt, _ *protocol.Verdict) {
			attempt.request.OwnerCapability = "stale"
		}},
		{"replay absent", func(summary *trialSummary, _ *auditedFencedAttempt, _ *protocol.Verdict) {
			summary.ReplayVerified = false
		}},
		{"wrong process", func(_ *trialSummary, attempt *auditedFencedAttempt, _ *protocol.Verdict) {
			attempt.process.Identity = "pid:999:start:boot:9"
		}},
		{"unsafe verdict", func(_ *trialSummary, _ *auditedFencedAttempt, verdict *protocol.Verdict) {
			verdict.Metrics.StaleActionAcceptCount = 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changedSummary := summary
			changedAuthority := *summary.Authority
			changedAuthority.Events = append([]workstore.Event(nil), summary.Authority.Events...)
			changedSummary.Authority = &changedAuthority
			changedSummary.WorkspaceEffects = append([]WorkspaceEffect(nil), summary.WorkspaceEffects...)
			changedAttempt := attempt
			changedVerdict := verdict
			test.change(&changedSummary, &changedAttempt, &changedVerdict)
			if err := validateFencedAuditTrial(manifest, input, changedVerdict, changedSummary, changedAttempt); err == nil {
				t.Fatal("invalid trial returned nil error")
			}
		})
	}
}

func TestWriteFencedEvidenceAuditRejectsOutputInsideSealedRoot(t *testing.T) {
	root := t.TempDir()
	report := FencedEvidenceAudit{EvidenceRoot: root, AllRequirementsVerified: true}
	if err := WriteFencedEvidenceAudit(filepath.Join(root, "audit.json"), report); err == nil {
		t.Fatal("audit output inside sealed evidence root returned nil error")
	}
}

func TestWriteEvidenceAuditRejectsSymlinkedParentIntoSealedRoot(t *testing.T) {
	root := t.TempDir()
	report := FencedEvidenceAudit{EvidenceRoot: root, AllRequirementsVerified: true}
	alias := filepath.Join(t.TempDir(), "output-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(alias, "escaped-audit.json")
	if err := WriteFencedEvidenceAudit(output, report); err == nil {
		t.Fatal("symlinked output parent into sealed root returned nil error")
	}
	if _, err := os.Lstat(filepath.Join(root, "escaped-audit.json")); !os.IsNotExist(err) {
		t.Fatalf("audit report was written inside sealed root: %v", err)
	}
}

func TestReadStrictJSONRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.json")
	if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if _, err := readStrictJSON[map[string]any](link); err == nil {
		t.Fatal("symlinked JSON artifact returned nil error")
	}
}

func TestCollectSourceIdentityRequiresHarnessAndStableHashes(t *testing.T) {
	const hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	input := protocol.EffectiveInput{
		AgentBinarySHA256: hashA,
		Settings: map[string]string{
			"harness_binary_sha256":  hashA,
			"worker_binary_sha256":   hashA,
			"effect_binary_sha256":   hashA,
			"launcher_binary_sha256": hashA,
		},
	}
	sources := make(map[string]string)
	if err := collectSourceIdentity(sources, input); err != nil {
		t.Fatalf("collect complete source identity: %v", err)
	}
	if sources["harness"] != hashA {
		t.Fatalf("harness source = %q, want %q", sources["harness"], hashA)
	}

	missing := input
	missing.Settings = map[string]string{
		"worker_binary_sha256": hashA, "effect_binary_sha256": hashA, "launcher_binary_sha256": hashA,
	}
	if err := collectSourceIdentity(make(map[string]string), missing); err == nil {
		t.Fatal("missing harness hash returned nil error")
	}

	changed := input
	changed.Settings = map[string]string{
		"harness_binary_sha256":  hashB,
		"worker_binary_sha256":   hashA,
		"effect_binary_sha256":   hashA,
		"launcher_binary_sha256": hashA,
	}
	if err := collectSourceIdentity(sources, changed); err == nil {
		t.Fatal("changed harness hash returned nil error")
	}
}

func TestCollectCompatibleSourceIdentityPreservesUniformHarnessMode(t *testing.T) {
	const (
		hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		hashC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		hashD = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		hashE = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	)
	withHarness := protocol.EffectiveInput{
		AgentBinarySHA256: hashA,
		Settings: map[string]string{
			"harness_binary_sha256":  hashB,
			"worker_binary_sha256":   hashC,
			"effect_binary_sha256":   hashD,
			"launcher_binary_sha256": hashE,
		},
	}
	sources := make(map[string]string)
	if err := collectCompatibleSourceIdentity(sources, withHarness, true); err != nil {
		t.Fatalf("collect harness-bound source: %v", err)
	}
	if sources["harness"] != hashB {
		t.Fatalf("harness source = %q, want %q", sources["harness"], hashB)
	}

	legacy := protocol.EffectiveInput{
		AgentBinarySHA256: hashA,
		Settings: map[string]string{
			"worker_binary_sha256":   hashC,
			"effect_binary_sha256":   hashD,
			"launcher_binary_sha256": hashE,
		},
	}
	if err := collectCompatibleSourceIdentity(make(map[string]string), legacy, false); err != nil {
		t.Fatalf("collect legacy source: %v", err)
	}
	if err := collectCompatibleSourceIdentity(make(map[string]string), withHarness, false); err == nil {
		t.Fatal("mixed legacy and harness-bound source returned nil error")
	}
	if err := collectCompatibleSourceIdentity(make(map[string]string), legacy, true); err == nil {
		t.Fatal("missing required harness source returned nil error")
	}
}
