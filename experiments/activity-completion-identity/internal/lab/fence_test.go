package lab

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestAttemptFencePersistsMonotonicOwnership(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "attempt-fence.db")
	fence, err := OpenAttemptFence(path)
	if err != nil {
		t.Fatalf("open fence: %v", err)
	}
	if err := fence.Register(ctx, 1, "owner-1"); err != nil {
		t.Fatalf("register attempt 1: %v", err)
	}
	if err := fence.Register(ctx, 2, "owner-2"); err != nil {
		t.Fatalf("register attempt 2: %v", err)
	}
	if err := fence.Close(); err != nil {
		t.Fatalf("close fence: %v", err)
	}

	fence, err = OpenAttemptFence(path)
	if err != nil {
		t.Fatalf("reopen fence: %v", err)
	}
	t.Cleanup(func() { _ = fence.Close() })
	if err := fence.Authorize(ctx, "owner-1"); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("authorize owner 1 error = %v; want ErrStaleAttempt", err)
	}
	if err := fence.Authorize(ctx, "owner-2"); err != nil {
		t.Fatalf("authorize owner 2: %v", err)
	}
}

func TestAttemptFenceRejectsOutOfOrderRegistration(t *testing.T) {
	fence, err := OpenAttemptFence(filepath.Join(t.TempDir(), "attempt-fence.db"))
	if err != nil {
		t.Fatalf("open fence: %v", err)
	}
	t.Cleanup(func() { _ = fence.Close() })
	ctx := context.Background()
	if err := fence.Register(ctx, 2, "owner-2"); err != nil {
		t.Fatalf("register attempt 2: %v", err)
	}
	if err := fence.Register(ctx, 1, "owner-1"); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("register attempt 1 error = %v; want ErrStaleAttempt", err)
	}
}

func TestAttemptFenceRejectsDifferentOwnerForSameAttempt(t *testing.T) {
	fence, err := OpenAttemptFence(filepath.Join(t.TempDir(), "attempt-fence.db"))
	if err != nil {
		t.Fatalf("open fence: %v", err)
	}
	t.Cleanup(func() { _ = fence.Close() })
	ctx := context.Background()
	if err := fence.Register(ctx, 2, "owner-2"); err != nil {
		t.Fatalf("register owner 2: %v", err)
	}
	if err := fence.Register(ctx, 2, "competitor"); !errors.Is(err, ErrOwnerConflict) {
		t.Fatalf("register competitor error = %v; want ErrOwnerConflict", err)
	}
	if err := fence.Register(ctx, 2, "owner-2"); err != nil {
		t.Fatalf("repeat owner registration: %v", err)
	}
}

func TestAttemptFenceRejectsInvalidAndCanceledRequests(t *testing.T) {
	fence, err := OpenAttemptFence(filepath.Join(t.TempDir(), "attempt-fence.db"))
	if err != nil {
		t.Fatalf("open fence: %v", err)
	}
	t.Cleanup(func() { _ = fence.Close() })
	ctx := context.Background()
	if err := fence.Register(ctx, 0, "owner"); err == nil {
		t.Fatal("zero attempt registration succeeded")
	}
	if err := fence.Register(ctx, 1, ""); err == nil {
		t.Fatal("empty owner registration succeeded")
	}
	if err := fence.Authorize(ctx, ""); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("empty owner authorization error = %v; want ErrStaleAttempt", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := fence.Register(canceled, 1, "owner"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled registration error = %v; want context.Canceled", err)
	}
}
