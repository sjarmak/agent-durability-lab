package lab

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDestinationMechanismsDistinguishUnsafeAndProtectedRetries(t *testing.T) {
	t.Parallel()
	for _, destination := range AllDestinations() {
		destination := destination
		for _, mode := range []Mode{ModeUnsafe, ModeProtected} {
			mode := mode
			t.Run(string(destination)+"/"+string(mode), func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				configuration, stop := prepareDestinationForTest(t, destination, root)
				defer stop()

				request := EffectRequest{
					EffectID: "effect-1", Payload: "agent-output-v1", Mode: mode,
				}
				first, err := applyEffect(context.Background(), destination, configuration, withAttempt(request, 1))
				if err != nil {
					t.Fatalf("apply attempt 1: %v", err)
				}
				second, err := applyEffect(context.Background(), destination, configuration, withAttempt(request, 2))
				if err != nil {
					t.Fatalf("apply attempt 2: %v", err)
				}
				state, err := snapshotDestination(context.Background(), destination, configuration)
				if err != nil {
					t.Fatalf("snapshot destination: %v", err)
				}

				wantCount := 2
				if mode == ModeProtected {
					wantCount = 1
					if second.Outcome != protectedOutcome(destination) {
						t.Errorf("attempt 2 outcome = %q, want %q", second.Outcome, protectedOutcome(destination))
					}
					if second.Receipt != first.Receipt {
						t.Errorf("protected receipts differ: %q != %q", first.Receipt, second.Receipt)
					}
				} else {
					if second.Outcome != OutcomeApplied {
						t.Errorf("unsafe attempt 2 outcome = %q, want applied", second.Outcome)
					}
					if second.Receipt == first.Receipt {
						t.Errorf("unsafe receipts unexpectedly match: %q", first.Receipt)
					}
				}
				if got := len(state.PhysicalEffects); got != wantCount {
					t.Errorf("physical effects = %d, want %d: %+v", got, wantCount, state)
				}
			})
		}
	}
}

func TestProtectedDestinationsRejectConflictingPayloadWithoutMutation(t *testing.T) {
	t.Parallel()
	for _, destination := range AllDestinations() {
		destination := destination
		t.Run(string(destination), func(t *testing.T) {
			t.Parallel()
			configuration, stop := prepareDestinationForTest(t, destination, t.TempDir())
			defer stop()
			request := EffectRequest{EffectID: "effect-1", Payload: "first", Mode: ModeProtected, Attempt: 1}
			if _, err := applyEffect(context.Background(), destination, configuration, request); err != nil {
				t.Fatalf("apply first payload: %v", err)
			}
			request.Payload = "conflict"
			request.Attempt = 2
			if _, err := applyEffect(context.Background(), destination, configuration, request); err == nil {
				t.Fatal("conflicting payload succeeded")
			}
			state, err := snapshotDestination(context.Background(), destination, configuration)
			if err != nil {
				t.Fatalf("snapshot after conflict: %v", err)
			}
			if len(state.PhysicalEffects) != 1 {
				t.Fatalf("conflict left %d physical effects: %+v", len(state.PhysicalEffects), state)
			}
		})
	}
}

func TestProtectedGitRejectsUncommittedMarker(t *testing.T) {
	t.Parallel()
	repositoryPath := filepath.Join(t.TempDir(), "repo")
	configuration := DestinationConfig{GitPath: repositoryPath}
	if err := prepareDestination(context.Background(), DestinationGit, configuration); err != nil {
		t.Fatalf("prepare Git: %v", err)
	}
	markerPath := filepath.Join(repositoryPath, "effects", "effect-1.txt")
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o750); err != nil {
		t.Fatalf("create marker directory: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write uncommitted marker: %v", err)
	}
	request := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 2}
	if _, err := applyEffect(context.Background(), DestinationGit, configuration, request); err == nil {
		t.Fatal("uncommitted marker was accepted as a reconciled effect")
	}
}

func TestProtectedGitRejectsConflictingMarker(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	configuration := DestinationConfig{GitPath: filepath.Join(root, "repo")}
	if err := prepareDestination(context.Background(), DestinationGit, configuration); err != nil {
		t.Fatalf("prepare Git: %v", err)
	}
	request := EffectRequest{EffectID: "effect-1", Payload: "first", Mode: ModeProtected, Attempt: 1}
	if _, err := applyEffect(context.Background(), DestinationGit, configuration, request); err != nil {
		t.Fatalf("apply first Git effect: %v", err)
	}
	request.Payload = "conflict"
	request.Attempt = 2
	if _, err := applyEffect(context.Background(), DestinationGit, configuration, request); err == nil {
		t.Fatal("conflicting Git marker succeeded")
	}
}

func TestArtifactPublicationLeavesNoTemporaryFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	configuration := DestinationConfig{ArtifactPath: root}
	if err := prepareDestination(context.Background(), DestinationArtifact, configuration); err != nil {
		t.Fatalf("prepare artifacts: %v", err)
	}
	request := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 1}
	for attempt := int32(1); attempt <= 2; attempt++ {
		request.Attempt = attempt
		if _, err := applyEffect(context.Background(), DestinationArtifact, configuration, request); err != nil {
			t.Fatalf("apply artifact attempt %d: %v", attempt, err)
		}
	}
	for _, directory := range []string{"blobs", "refs"} {
		entries, err := os.ReadDir(filepath.Join(root, directory))
		if err != nil {
			t.Fatalf("read %s: %v", directory, err)
		}
		for _, entry := range entries {
			if entry.Name()[0] == '.' {
				t.Errorf("temporary artifact remains: %s/%s", directory, entry.Name())
			}
		}
	}
}

func TestGitBundleDestinationIsRelativeToCaller(t *testing.T) {
	evidenceDirectory, err := os.MkdirTemp(".", ".git-bundle-evidence-*")
	if err != nil {
		t.Fatalf("create caller-relative evidence directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(evidenceDirectory); err != nil {
			t.Errorf("remove evidence directory: %v", err)
		}
	})
	repositoryPath := filepath.Join(t.TempDir(), "repo")
	configuration := DestinationConfig{GitPath: repositoryPath}
	if err := prepareDestination(context.Background(), DestinationGit, configuration); err != nil {
		t.Fatalf("prepare Git: %v", err)
	}
	request := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 1}
	if _, err := applyEffect(context.Background(), DestinationGit, configuration, request); err != nil {
		t.Fatalf("apply Git effect: %v", err)
	}
	bundlePath := filepath.Join(evidenceDirectory, "destination.git.bundle")
	if err := exportGitBundle(context.Background(), repositoryPath, bundlePath); err != nil {
		t.Fatalf("export caller-relative bundle: %v", err)
	}
	if _, err := os.Stat(bundlePath); err != nil {
		t.Fatalf("stat caller-relative bundle: %v", err)
	}
}

func prepareDestinationForTest(t *testing.T, destination Destination, root string) (DestinationConfig, func()) {
	t.Helper()
	configuration := DestinationConfig{
		DatabasePath: filepath.Join(root, "effects.db"),
		GitPath:      filepath.Join(root, "repo"),
		ArtifactPath: filepath.Join(root, "artifacts"),
	}
	var stop func()
	if destination == DestinationIdempotentAPI || destination == DestinationNonIdempotentAPI ||
		destination == DestinationMessage {
		service, err := StartHTTPDestination()
		if err != nil {
			t.Fatalf("start HTTP destination: %v", err)
		}
		configuration.HTTPURL = service.URL()
		stop = service.Close
	} else {
		stop = func() {}
	}
	if err := prepareDestination(context.Background(), destination, configuration); err != nil {
		t.Fatalf("prepare destination: %v", err)
	}
	return configuration, stop
}

func withAttempt(request EffectRequest, attempt int32) EffectRequest {
	request.Attempt = attempt
	return request
}
