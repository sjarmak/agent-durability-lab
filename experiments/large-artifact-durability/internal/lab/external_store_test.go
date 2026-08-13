package lab

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func TestExternalStorageProtectedDriverReusesObjectAfterClaimFailure(t *testing.T) {
	t.Parallel()

	for _, mode := range []Mode{ModeProtected, ModeUnsafe} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			payload := &commonpb.Payload{
				Metadata: map[string][]byte{"encoding": []byte("json/plain")},
				Data:     testArtifactRequest(mode, 1).Content,
			}
			first, err := NewFileStorageDriver(root, mode, func(_ context.Context, boundary Boundary, snapshot StoreSnapshot) error {
				if boundary != BoundaryExternalStorageStored || len(snapshot.Blobs) != 1 {
					t.Fatalf("external-store boundary = %q, snapshot = %+v", boundary, snapshot)
				}
				return errInjectedCrash
			})
			if err != nil {
				t.Fatalf("NewFileStorageDriver: %v", err)
			}
			_, err = first.Store(converter.StorageDriverStoreContext{Context: context.Background()}, []*commonpb.Payload{payload})
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("first Store error = %v, want injected crash", err)
			}

			retry, err := NewFileStorageDriver(root, mode, nil)
			if err != nil {
				t.Fatalf("retry NewFileStorageDriver: %v", err)
			}
			claims, err := retry.Store(converter.StorageDriverStoreContext{Context: context.Background()}, []*commonpb.Payload{payload})
			if err != nil {
				t.Fatalf("retry Store: %v", err)
			}
			if len(claims) != 1 || claims[0].ClaimData["sha256"] == "" || claims[0].ClaimData["key"] == "" {
				t.Fatalf("claims = %+v", claims)
			}
			objects, err := retry.Snapshot(context.Background())
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			want := 1
			if mode == ModeUnsafe {
				want = 2
			}
			if len(objects.Blobs) != want {
				t.Fatalf("stored objects = %d, want %d", len(objects.Blobs), want)
			}

			retrieved, err := retry.Retrieve(converter.StorageDriverRetrieveContext{Context: context.Background()}, claims)
			if err != nil {
				t.Fatalf("Retrieve: %v", err)
			}
			if len(retrieved) != 1 || string(retrieved[0].GetData()) != string(payload.GetData()) {
				t.Fatal("retrieved payload differs from stored payload")
			}
		})
	}
}

func TestExternalStorageRejectsSymlinkedObjectDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, externalObjectsDirectory)); err != nil {
		t.Fatalf("create object-directory symlink: %v", err)
	}
	if _, err := NewFileStorageDriver(root, ModeProtected, nil); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("NewFileStorageDriver error = %v, want ErrInvalidArtifact", err)
	}
}

func TestExternalStorageRejectsMalformedInputsAndObjects(t *testing.T) {
	t.Parallel()

	driver, err := NewFileStorageDriver(t.TempDir(), ModeProtected, nil)
	if err != nil {
		t.Fatalf("NewFileStorageDriver: %v", err)
	}
	if _, err := driver.Store(converter.StorageDriverStoreContext{}, []*commonpb.Payload{{Data: []byte("x")}}); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("nil-context Store error = %v", err)
	}
	if _, err := NewFileStorageDriver("", ModeProtected, nil); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("empty-root driver error = %v, want ErrInvalidArtifact", err)
	}
	if _, err := driver.Store(converter.StorageDriverStoreContext{Context: context.Background()}, []*commonpb.Payload{nil}); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("nil-payload Store error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := driver.Store(converter.StorageDriverStoreContext{Context: canceled}, []*commonpb.Payload{{Data: []byte("x")}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Store error = %v", err)
	}
	if _, err := driver.Snapshot(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Snapshot error = %v", err)
	}

	claim, err := driver.Store(converter.StorageDriverStoreContext{Context: context.Background()}, []*commonpb.Payload{{Data: []byte("payload")}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	for name, mutate := range map[string]func(converter.StorageDriverClaim) converter.StorageDriverClaim{
		"missing field": func(value converter.StorageDriverClaim) converter.StorageDriverClaim {
			delete(value.ClaimData, "size")
			return value
		},
		"traversal key": func(value converter.StorageDriverClaim) converter.StorageDriverClaim {
			value.ClaimData["key"] = "../outside"
			return value
		},
		"noncanonical digest": func(value converter.StorageDriverClaim) converter.StorageDriverClaim {
			value.ClaimData["sha256"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
			return value
		},
		"oversized claim": func(value converter.StorageDriverClaim) converter.StorageDriverClaim {
			value.ClaimData["size"] = "999999999"
			return value
		},
	} {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			copyClaim := converter.StorageDriverClaim{ClaimData: map[string]string{}}
			for key, value := range claim[0].ClaimData {
				copyClaim.ClaimData[key] = value
			}
			if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{Context: context.Background()}, []converter.StorageDriverClaim{mutate(copyClaim)}); !errors.Is(err, ErrInvalidArtifact) {
				t.Fatalf("Retrieve error = %v, want ErrInvalidArtifact", err)
			}
		})
	}
	objectPath := filepath.Join(driver.root, externalObjectsDirectory, claim[0].ClaimData["key"])
	if err := os.Remove(objectPath); err != nil {
		t.Fatalf("remove object: %v", err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), objectPath); err != nil {
		t.Fatalf("replace object with symlink: %v", err)
	}
	if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{Context: context.Background()}, claim); err == nil {
		t.Fatal("symlinked external object accepted")
	}
}

func TestExternalStorageDriverRejectsTamperedClaim(t *testing.T) {
	t.Parallel()

	driver, err := NewFileStorageDriver(t.TempDir(), ModeProtected, nil)
	if err != nil {
		t.Fatalf("NewFileStorageDriver: %v", err)
	}
	claims, err := driver.Store(converter.StorageDriverStoreContext{Context: context.Background()}, []*commonpb.Payload{{Data: []byte("payload")}})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	claims[0].ClaimData["sha256"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := driver.Retrieve(converter.StorageDriverRetrieveContext{Context: context.Background()}, claims); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("Retrieve error = %v, want ErrArtifactConflict", err)
	}
}
