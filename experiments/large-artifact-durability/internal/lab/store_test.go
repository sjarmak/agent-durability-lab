package lab

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var errInjectedCrash = errors.New("injected crash")

func TestProtectedProduceRecoversEveryPublicationBoundary(t *testing.T) {
	t.Parallel()

	for _, boundary := range []Boundary{
		BoundaryBlobPublished,
		BoundaryReferenceCreated,
		BoundaryReferencePublished,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			request := testArtifactRequest(ModeProtected, 1)
			_, err := store.Produce(context.Background(), request, failAt(boundary))
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("Produce error = %v, want injected crash", err)
			}

			beforeRetry := snapshotStore(t, store)
			assertBoundaryState(t, boundary, beforeRetry)

			request.Attempt = 2
			reference, err := store.Produce(context.Background(), request, nil)
			if err != nil {
				t.Fatalf("retry Produce: %v", err)
			}
			if reference.LogicalID != request.LogicalID || reference.Digest == "" || reference.Size != int64(len(request.Content)) {
				t.Fatalf("unexpected reference: %+v", reference)
			}

			report, err := store.Reconcile(context.Background())
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if report.ReachableBlobs != 1 {
				t.Fatalf("reachable blobs = %d, want 1", report.ReachableBlobs)
			}
			final := snapshotStore(t, store)
			if len(final.Blobs) != 1 || len(final.References) != 1 || len(final.PendingReferences) != 0 {
				t.Fatalf("final protected inventory = %+v", final)
			}
			if !bytes.Equal(readArtifact(t, store, reference), request.Content) {
				t.Fatal("retrieved artifact differs from original content")
			}
		})
	}
}

func TestArtifactStoreRejectsInvalidRequestsAndDurableTampering(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	valid := testArtifactRequest(ModeProtected, 1)
	for name, mutate := range map[string]func(*ProduceRequest){
		"mode":       func(request *ProduceRequest) { request.Mode = "other" },
		"logical ID": func(request *ProduceRequest) { request.LogicalID = "../escape" },
		"attempt":    func(request *ProduceRequest) { request.Attempt = 0 },
		"empty":      func(request *ProduceRequest) { request.Content = nil },
		"oversized":  func(request *ProduceRequest) { request.Content = make([]byte, MaxArtifactBytes+1) },
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if _, err := store.Produce(context.Background(), request, nil); !errors.Is(err, ErrInvalidArtifact) {
				t.Fatalf("Produce error = %v, want ErrInvalidArtifact", err)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Produce(canceled, valid, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Produce error = %v", err)
	}

	reference, err := store.Produce(context.Background(), valid, nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if _, err := store.Acknowledge(context.Background(), AcknowledgeRequest{Reference: reference, ConsumerID: "../consumer", Attempt: 1, Mode: ModeProtected}, nil); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("invalid Acknowledge error = %v", err)
	}
	if _, err := store.Read(canceled, reference); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Read error = %v", err)
	}
	if _, err := store.Snapshot(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Snapshot error = %v", err)
	}
	if _, err := store.Reconcile(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Reconcile error = %v", err)
	}
	invalidReference := reference
	invalidReference.Digest = "not-a-digest"
	if _, err := store.Read(context.Background(), invalidReference); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("invalid Read error = %v, want ErrInvalidArtifact", err)
	}

	blobPath := filepath.Join(store.root, blobsDirectory, reference.BlobName)
	if err := os.WriteFile(blobPath, []byte("tampered"), 0o600); err != nil {
		t.Fatalf("tamper blob: %v", err)
	}
	if _, err := store.Read(context.Background(), reference); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("tampered Read error = %v, want ErrArtifactConflict", err)
	}
}

func TestProtectedAcknowledgementRejectsConflictingDurableReceipt(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	reference, err := store.Produce(context.Background(), testArtifactRequest(ModeProtected, 1), nil)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	request := AcknowledgeRequest{Reference: reference, ConsumerID: "consumer-1", Attempt: 1, Mode: ModeProtected}
	if _, err := store.Acknowledge(context.Background(), request, nil); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	path := filepath.Join(store.root, acknowledgementsDirectory, "artifact-1--consumer-1.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("tamper acknowledgement: %v", err)
	}
	request.Attempt = 2
	if _, err := store.Acknowledge(context.Background(), request, nil); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting acknowledgement error = %v, want ErrArtifactConflict", err)
	}
}

func TestArtifactStoreRejectsSymlinkedSubdirectoryAndUnexpectedInventory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(root, blobsDirectory)); err != nil {
		t.Fatalf("create blobs symlink: %v", err)
	}
	if _, err := NewArtifactStore(root); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("NewArtifactStore error = %v, want ErrInvalidArtifact", err)
	}

	store := newTestStore(t)
	if err := os.WriteFile(filepath.Join(store.root, referencesDirectory, "unexpected.txt"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write unexpected reference: %v", err)
	}
	if _, err := store.Reconcile(context.Background()); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("Reconcile error = %v, want ErrInvalidArtifact", err)
	}
	if err := os.Remove(filepath.Join(store.root, referencesDirectory, "unexpected.txt")); err != nil {
		t.Fatalf("remove unexpected reference: %v", err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(store.root, pendingDirectory, "pending")); err != nil {
		t.Fatalf("create pending symlink: %v", err)
	}
	if _, err := store.Reconcile(context.Background()); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("Reconcile pending error = %v, want ErrInvalidArtifact", err)
	}
}

func TestUnsafeProduceDuplicatesPublishedReferenceAfterRedelivery(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	request := testArtifactRequest(ModeUnsafe, 1)
	if _, err := store.Produce(context.Background(), request, failAt(BoundaryReferencePublished)); !errors.Is(err, errInjectedCrash) {
		t.Fatalf("first Produce error = %v, want injected crash", err)
	}
	request.Attempt = 2
	if _, err := store.Produce(context.Background(), request, nil); err != nil {
		t.Fatalf("retry Produce: %v", err)
	}

	final := snapshotStore(t, store)
	if len(final.Blobs) != 2 || len(final.References) != 2 {
		t.Fatalf("unsafe inventory = %+v, want two blobs and two references", final)
	}
}

func TestProtectedAcknowledgementDeduplicatesAfterRedelivery(t *testing.T) {
	t.Parallel()

	for _, mode := range []Mode{ModeProtected, ModeUnsafe} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			request := testArtifactRequest(mode, 1)
			reference, err := store.Produce(context.Background(), request, nil)
			if err != nil {
				t.Fatalf("Produce: %v", err)
			}
			ackRequest := AcknowledgeRequest{
				Reference:  reference,
				ConsumerID: "consumer-1",
				Attempt:    1,
				Mode:       mode,
			}
			if _, err := store.Acknowledge(context.Background(), ackRequest, failAt(BoundaryAcknowledgementPublished)); !errors.Is(err, errInjectedCrash) {
				t.Fatalf("first Acknowledge error = %v, want injected crash", err)
			}
			ackRequest.Attempt = 2
			acknowledgement, err := store.Acknowledge(context.Background(), ackRequest, nil)
			if err != nil {
				t.Fatalf("retry Acknowledge: %v", err)
			}
			if acknowledgement.Digest != reference.Digest || acknowledgement.ConsumerID != ackRequest.ConsumerID {
				t.Fatalf("unexpected acknowledgement: %+v", acknowledgement)
			}

			count := len(snapshotStore(t, store).Acknowledgements)
			want := 1
			if mode == ModeUnsafe {
				want = 2
			}
			if count != want {
				t.Fatalf("acknowledgement count = %d, want %d", count, want)
			}
		})
	}
}

func TestProtectedReferenceRejectsConflictingContent(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	request := testArtifactRequest(ModeProtected, 1)
	if _, err := store.Produce(context.Background(), request, nil); err != nil {
		t.Fatalf("first Produce: %v", err)
	}
	request.Attempt = 2
	request.Content = append([]byte(nil), request.Content...)
	request.Content[0] ^= 0xff
	if _, err := store.Produce(context.Background(), request, nil); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting Produce error = %v, want ErrArtifactConflict", err)
	}
}

func TestReconcileRemovesUnreachableBlobAndPendingReference(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	request := testArtifactRequest(ModeUnsafe, 1)
	if _, err := store.Produce(context.Background(), request, failAt(BoundaryReferenceCreated)); !errors.Is(err, errInjectedCrash) {
		t.Fatalf("Produce error = %v, want injected crash", err)
	}
	before := snapshotStore(t, store)
	if len(before.Blobs) != 1 || len(before.PendingReferences) != 1 || len(before.References) != 0 {
		t.Fatalf("pre-reconcile inventory = %+v", before)
	}

	report, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(report.RemovedBlobs) != 1 || len(report.RemovedPendingReferences) != 1 {
		t.Fatalf("reconciliation report = %+v", report)
	}
	final := snapshotStore(t, store)
	if len(final.Blobs) != 0 || len(final.PendingReferences) != 0 {
		t.Fatalf("post-reconcile inventory = %+v", final)
	}
}

func newTestStore(t *testing.T) *ArtifactStore {
	t.Helper()
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	return store
}

func testArtifactRequest(mode Mode, attempt int32) ProduceRequest {
	return ProduceRequest{
		LogicalID: "artifact-1",
		Content:   bytes.Repeat([]byte("large-agent-artifact\n"), 32*1024),
		Attempt:   attempt,
		Mode:      mode,
	}
}

func failAt(target Boundary) BoundaryHook {
	return func(_ context.Context, observed Boundary, _ StoreSnapshot) error {
		if observed == target {
			return errInjectedCrash
		}
		return nil
	}
}

func snapshotStore(t *testing.T, store *ArtifactStore) StoreSnapshot {
	t.Helper()
	snapshot, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return snapshot
}

func readArtifact(t *testing.T, store *ArtifactStore, reference ArtifactReference) []byte {
	t.Helper()
	content, err := store.Read(context.Background(), reference)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return content
}

func assertBoundaryState(t *testing.T, boundary Boundary, snapshot StoreSnapshot) {
	t.Helper()
	if len(snapshot.Blobs) != 1 {
		t.Fatalf("%s blobs = %d, want 1", boundary, len(snapshot.Blobs))
	}
	switch boundary {
	case BoundaryBlobPublished:
		if len(snapshot.PendingReferences) != 0 || len(snapshot.References) != 0 {
			t.Fatalf("blob boundary inventory = %+v", snapshot)
		}
	case BoundaryReferenceCreated:
		if len(snapshot.PendingReferences) != 1 || len(snapshot.References) != 0 {
			t.Fatalf("reference-created inventory = %+v", snapshot)
		}
	case BoundaryReferencePublished:
		if len(snapshot.PendingReferences) != 0 || len(snapshot.References) != 1 {
			t.Fatalf("reference-published inventory = %+v", snapshot)
		}
	default:
		t.Fatalf("unexpected boundary %q", boundary)
	}
}
