package provider

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/temporal-community/sandbox-orchestration-harness/sdk/compute"
)

func TestProviderConfigurationAndCommandValidation(t *testing.T) {
	t.Parallel()
	valid := map[string]string{
		configDatabasePath: "/tmp/provider.db", configMode: string(ModeIdempotent),
		configSessionID: "session", configWorkerIdentity: "worker",
	}
	if config, err := parseConfig(valid); err != nil || config.Mode != ModeIdempotent {
		t.Fatalf("parseConfig(valid) = %+v, %v", config, err)
	}
	invalid := []map[string]string{
		{},
		withConfig(valid, configGeneration, "not-a-number"),
		withConfig(valid, configBarrierURL, "http://barrier"),
		withConfigs(valid, map[string]string{configBarrierURL: "http://barrier", configFaultOperation: "invalid"}),
		withConfig(valid, configMode, string(ModeFenced)),
	}
	for _, raw := range invalid {
		if _, err := parseConfig(raw); err == nil {
			t.Fatalf("parseConfig(%v) error = nil", raw)
		}
	}
	if _, err := newControlledProvider(valid); err == nil {
		t.Fatal("newControlledProvider() with missing database error = nil")
	}
	if _, err := EncodeCommand(CommandEnvelope{}); err == nil {
		t.Fatal("EncodeCommand(empty) error = nil")
	}
	encoded, err := EncodeCommand(CommandEnvelope{LogicalEffectID: "effect", Payload: "payload"})
	if err != nil {
		t.Fatalf("EncodeCommand(valid) error = %v", err)
	}
	if decoded, err := decodeCommand(encoded); err != nil || decoded.LogicalEffectID != "effect" {
		t.Fatalf("decodeCommand(valid) = %+v, %v", decoded, err)
	}
	for _, value := range []string{
		`{"unknown":true}`,
		`{"logical_effect_id":"effect"} {"trailing":true}`,
		`{"payload":"missing identity"}`,
	} {
		if _, err := decodeCommand(value); err == nil {
			t.Fatalf("decodeCommand(%q) error = nil", value)
		}
	}
}

func TestProviderNilAndUnsupportedOperations(t *testing.T) {
	t.Parallel()
	value := &controlledProvider{}
	if err := value.Suspend(context.Background(), nil); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Suspend() error = %v", err)
	}
	if err := value.Resume(context.Background(), nil); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("Resume() error = %v", err)
	}
	if err := value.DeleteSnapshot(context.Background(), nil); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if err := value.Stop(context.Background(), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Stop(nil) error = %v", err)
	}
	if _, _, err := value.Snapshot(context.Background(), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Snapshot(nil) error = %v", err)
	}
	if _, err := value.StartFromSnapshot(context.Background(), "queue", nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("StartFromSnapshot(nil) error = %v", err)
	}
	if _, err := value.ExecuteCommand(context.Background(), nil, "{}"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ExecuteCommand(nil) error = %v", err)
	}
	if _, err := value.ExecuteCommand(context.Background(), &compute.ProviderStatus{InstanceID: "instance"}, "invalid"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ExecuteCommand(invalid) error = %v", err)
	}
}

func TestStoreBoundaryErrorsAndPhysicalAttemptReplay(t *testing.T) {
	t.Parallel()
	if _, err := Create("", ModeUnsafe); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Create(empty) error = %v", err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "missing.db")); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("Open(missing) error = %v", err)
	}
	store := createStore(t, ModeIdempotent)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.SetAuthority(canceled, Authority{Generation: 1, Capability: "cap"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetAuthority(canceled) error = %v", err)
	}
	if err := store.SetAuthority(context.Background(), Authority{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("SetAuthority(empty) error = %v", err)
	}
	if _, err := store.Apply(canceled, Request{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply(canceled) error = %v", err)
	}
	if _, err := store.Apply(context.Background(), Request{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Apply(empty) error = %v", err)
	}
	if _, err := store.Snapshot(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot(canceled) error = %v", err)
	}
	first := Request{Kind: OperationStart, OperationID: "operation-1", PhysicalAttemptID: "physical-1"}
	result := apply(t, store, first)
	replayed := apply(t, store, first)
	if replayed.ReceiptID != result.ReceiptID {
		t.Fatalf("physical replay receipt = %q, want %q", replayed.ReceiptID, result.ReceiptID)
	}
	conflict := first
	conflict.OperationID = "operation-2"
	if _, err := store.Apply(context.Background(), conflict); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("physical identity conflict error = %v", err)
	}
	if got := rejectionError("stale_authority"); !errors.Is(got, ErrStaleAuthority) {
		t.Fatalf("rejectionError(stale) = %v", got)
	}
}

func withConfig(original map[string]string, key, value string) map[string]string {
	return withConfigs(original, map[string]string{key: value})
}

func withConfigs(original map[string]string, changes map[string]string) map[string]string {
	next := make(map[string]string, len(original)+len(changes))
	for key, value := range original {
		next[key] = value
	}
	for key, value := range changes {
		next[key] = value
	}
	return next
}
