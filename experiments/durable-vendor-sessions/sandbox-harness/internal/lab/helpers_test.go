package lab

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/sandbox-harness/internal/provider"
)

func TestValidateOptionsAndEvidenceRoot(t *testing.T) {
	t.Parallel()
	valid := Options{EvidenceRoot: filepath.Join(t.TempDir(), "evidence"), TemporalPath: "/temporal", Trials: 3, Timeout: time.Second}
	if err := validateOptions(valid); err != nil {
		t.Fatalf("validateOptions(valid) error = %v", err)
	}
	for _, invalid := range []Options{
		{},
		{EvidenceRoot: valid.EvidenceRoot, TemporalPath: valid.TemporalPath, Trials: 2, Timeout: time.Second},
		{EvidenceRoot: valid.EvidenceRoot, TemporalPath: valid.TemporalPath, Trials: 3},
	} {
		if err := validateOptions(invalid); err == nil {
			t.Fatalf("validateOptions(%+v) error = nil", invalid)
		}
	}
	if err := createEvidenceRoot(valid.EvidenceRoot); err != nil {
		t.Fatalf("createEvidenceRoot() error = %v", err)
	}
	if err := createEvidenceRoot(valid.EvidenceRoot); err == nil {
		t.Fatal("second createEvidenceRoot() error = nil")
	}
}

func TestAmbiguousAttemptVerificationAndSelection(t *testing.T) {
	t.Parallel()
	base := []provider.Attempt{
		{Kind: provider.OperationCommand, OperationID: "operation", PhysicalAttemptID: "attempt-1", LogicalEffectID: "effect", Applied: true},
		{Kind: provider.OperationCommand, OperationID: "operation", PhysicalAttemptID: "attempt-2", LogicalEffectID: "effect", Applied: false},
		{Kind: provider.OperationStop, OperationID: "stop", PhysicalAttemptID: "stop-1", Applied: true},
	}
	if got := operationAttempts(base, provider.OperationCommand); len(got) != 2 {
		t.Fatalf("operationAttempts() = %d, want 2", len(got))
	}
	if got := attemptsForEffect(base, "effect"); len(got) != 2 {
		t.Fatalf("attemptsForEffect() = %d, want 2", len(got))
	}
	if err := verifyAmbiguousAttempts(protocol.ProbeProtected, base[:2]); err != nil {
		t.Fatalf("protected attempts error = %v", err)
	}
	unsafe := append([]provider.Attempt(nil), base[:2]...)
	unsafe[1].Applied = true
	if err := verifyAmbiguousAttempts(protocol.ProbeUnsafe, unsafe); err != nil {
		t.Fatalf("unsafe attempts error = %v", err)
	}
	invalid := [][]provider.Attempt{
		base[:1],
		{{OperationID: "one", PhysicalAttemptID: "same", Applied: true}, {OperationID: "two", PhysicalAttemptID: "same", Applied: false}},
		{{OperationID: "one", PhysicalAttemptID: "one"}, {OperationID: "one", PhysicalAttemptID: "two"}},
	}
	for _, attempts := range invalid {
		if err := verifyAmbiguousAttempts(protocol.ProbeProtected, attempts); err == nil {
			t.Fatalf("verifyAmbiguousAttempts(%+v) error = nil", attempts)
		}
	}
}

func TestReconcileActiveInstancesAndExclusiveWrites(t *testing.T) {
	t.Parallel()
	store, err := provider.Create(filepath.Join(t.TempDir(), "provider.db"), provider.ModeIdempotent)
	if err != nil {
		t.Fatalf("provider.Create() error = %v", err)
	}
	started, err := store.Apply(context.Background(), provider.Request{
		Kind: provider.OperationStart, OperationID: "start", PhysicalAttemptID: "start-1",
	})
	if err != nil {
		t.Fatalf("start provider instance: %v", err)
	}
	before, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("provider Snapshot() error = %v", err)
	}
	if activeInstanceCount(before) != 1 || before.Instance(started.InstanceID).InstanceID == "" {
		t.Fatalf("active provider state = %+v", before)
	}
	receipt, err := reconcileActiveInstances(context.Background(), store, "session", before)
	if err != nil || receipt == "" {
		t.Fatalf("reconcileActiveInstances() = %q, %v", receipt, err)
	}
	after, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("provider Snapshot() after reconcile error = %v", err)
	}
	if activeInstanceCount(after) != 0 {
		t.Fatalf("active instances after reconcile = %d", activeInstanceCount(after))
	}
	if _, err := reconcileActiveInstances(context.Background(), store, "session", after); err == nil {
		t.Fatal("reconcile with no active instance error = nil")
	}

	path := filepath.Join(t.TempDir(), "evidence.json")
	if err := writeExclusive(path, []byte("evidence")); err != nil {
		t.Fatalf("writeExclusive() error = %v", err)
	}
	if contents, err := os.ReadFile(path); err != nil || string(contents) != "evidence" {
		t.Fatalf("exclusive contents = %q, %v", contents, err)
	}
	if err := writeExclusive(path, []byte("replacement")); err == nil {
		t.Fatal("second writeExclusive() error = nil")
	}
	if digest, err := executableSHA256(); err != nil || len(digest) != 64 {
		t.Fatalf("executableSHA256() = %q, %v", digest, err)
	}
}
