package lab

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestinationBoundaryRejectsMalformedRequests(t *testing.T) {
	t.Parallel()
	destination, err := StartHTTPDestination()
	if err != nil {
		t.Fatalf("start HTTP destination: %v", err)
	}
	defer destination.Close()

	requests := []string{
		`{"effect_id":`,
		`{"effect_id":"effect-1","payload":"payload","mode":"protected","attempt":1,"extra":true}`,
		`{"effect_id":"effect-1","payload":"payload","mode":"protected","attempt":1}{}`,
		`{"effect_id":"","payload":"payload","mode":"protected","attempt":1}`,
		`{"effect_id":"effect-1","payload":"` + strings.Repeat("x", maxDestinationRequestBytes) + `","mode":"protected","attempt":1}`,
	}
	for _, body := range requests {
		response, err := http.Post(destination.URL()+"/v1/idempotent-api", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("post malformed request: %v", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("malformed request status = %d, want %d", response.StatusCode, http.StatusBadRequest)
		}
	}

	response, err := http.Get(destination.URL() + "/v1/state/unknown")
	if err != nil {
		t.Fatalf("get invalid state: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid state status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestDestinationConfigurationAndRequestsFailClosed(t *testing.T) {
	t.Parallel()
	for name, operation := range map[string]func() error{
		"HTTP URL": func() error {
			return prepareDestination(context.Background(), DestinationMessage, DestinationConfig{})
		},
		"database path": func() error {
			return prepareDestination(context.Background(), DestinationDatabase, DestinationConfig{})
		},
		"Git path": func() error {
			return prepareDestination(context.Background(), DestinationGit, DestinationConfig{})
		},
		"artifact path": func() error {
			return prepareDestination(context.Background(), DestinationArtifact, DestinationConfig{})
		},
		"unknown destination": func() error {
			return prepareDestination(context.Background(), "unknown", DestinationConfig{})
		},
		"unknown snapshot": func() error {
			_, err := snapshotDestination(context.Background(), "unknown", DestinationConfig{})
			return err
		},
	} {
		name, operation := name, operation
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := operation(); err == nil {
				t.Fatal("invalid input succeeded")
			}
		})
	}

	base := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 1}
	for name, mutate := range map[string]func(*EffectRequest){
		"destination": func(request *EffectRequest) {},
		"mode":        func(request *EffectRequest) { request.Mode = "unknown" },
		"effect ID":   func(request *EffectRequest) { request.EffectID = "" },
		"unsafe ID":   func(request *EffectRequest) { request.EffectID = "../escape" },
		"payload":     func(request *EffectRequest) { request.Payload = "" },
		"attempt":     func(request *EffectRequest) { request.Attempt = 0 },
	} {
		name, mutate := name, mutate
		t.Run("request "+name, func(t *testing.T) {
			t.Parallel()
			request := base
			mutate(&request)
			destination := DestinationDatabase
			if name == "destination" {
				destination = "unknown"
			}
			if _, err := applyEffect(context.Background(), destination, DestinationConfig{}, request); err == nil {
				t.Fatal("invalid effect request succeeded")
			}
		})
	}
}

func TestProtectedArtifactReconstructsMissingBlobFromReference(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := prepareArtifactDestination(root); err != nil {
		t.Fatalf("prepare artifact destination: %v", err)
	}
	request := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 1}
	first, err := applyArtifactEffect(root, request)
	if err != nil {
		t.Fatalf("apply artifact: %v", err)
	}
	blobPath := filepath.Join(root, "blobs", strings.TrimPrefix(first.Receipt, "artifact:"))
	if err := os.Remove(blobPath); err != nil {
		t.Fatalf("remove blob to simulate partial durability: %v", err)
	}
	request.Attempt = 2
	second, err := applyArtifactEffect(root, request)
	if err != nil {
		t.Fatalf("reconcile artifact: %v", err)
	}
	if second.Outcome != OutcomeReconciled || second.Receipt != first.Receipt {
		t.Fatalf("reconciled result = %+v, want same receipt", second)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("reconstructed blob: %v", err)
	}
}

func TestEvidenceWritersRejectImpossibleOutput(t *testing.T) {
	t.Parallel()
	missingParent := filepath.Join(t.TempDir(), "missing", "evidence.json")
	if err := writeFileAtomically(missingParent, []byte("evidence")); err == nil {
		t.Fatal("atomic write with missing parent succeeded")
	}
	if err := writeJSON(filepath.Join(t.TempDir(), "evidence.json"), make(chan int)); err == nil {
		t.Fatal("JSON writer accepted an unsupported value")
	}
}

func TestHTTPDestinationClientsPropagateCancellationAndInvalidURLs(t *testing.T) {
	t.Parallel()
	request := EffectRequest{EffectID: "effect-1", Payload: "payload", Mode: ModeProtected, Attempt: 1}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := applyHTTPEffect(ctx, DestinationIdempotentAPI, "http://127.0.0.1:1", request); err == nil {
		t.Fatal("canceled HTTP mutation succeeded")
	}
	if _, err := applyHTTPEffect(context.Background(), DestinationIdempotentAPI, "://bad", request); err == nil {
		t.Fatal("invalid HTTP mutation URL succeeded")
	}
	if _, _, err := reconcileHTTP(context.Background(), "://bad", request.EffectID, request.Payload); err == nil {
		t.Fatal("invalid reconciliation URL succeeded")
	}
	if _, err := snapshotHTTPDestination(ctx, "http://127.0.0.1:1", DestinationIdempotentAPI); err == nil {
		t.Fatal("canceled HTTP snapshot succeeded")
	}
}

func TestEvidenceHelpersReportUnavailableVersionsAndInvalidRepository(t *testing.T) {
	t.Parallel()
	if got := moduleVersion("example.invalid/not-a-module"); got != "unknown" {
		t.Fatalf("unknown module version = %q", got)
	}
	if got := temporalCLIVersion(context.Background(), filepath.Join(t.TempDir(), "missing-temporal")); !strings.HasPrefix(got, "unknown: ") {
		t.Fatalf("missing Temporal version = %q", got)
	}
	if err := exportGitBundle(
		context.Background(), filepath.Join(t.TempDir(), "missing-repository"), filepath.Join(t.TempDir(), "bundle"),
	); err == nil {
		t.Fatal("Git bundle export from missing repository succeeded")
	}
}
