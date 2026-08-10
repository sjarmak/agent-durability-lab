//go:build linux

package semantics

import (
	"os"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestCleanupControlledProcessTreatsReusedPIDAsSafeNoOp(t *testing.T) {
	identity, err := agentprocess.CaptureIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	identity.StartIdentity = "deliberately-not-the-current-process-start"

	err = cleanupControlledProcess(controlledProcess{
		lease: workstore.Lease{SessionID: "cleanup-test", Generation: 1, OwnerToken: "owner-token"},
		process: agentprocess.Process{
			PID: identity.PID, StartIdentity: identity.StartIdentity, ProcessGroupID: identity.ProcessGroupID,
		},
	})
	if err != nil {
		t.Fatalf("cleanup of reused PID = %v; want safe no-op", err)
	}
}

func TestFileSHA256FailsClosedWhenExecutableIsMissing(t *testing.T) {
	if _, err := fileSHA256(t.TempDir() + "/missing"); err == nil {
		t.Fatal("fileSHA256(missing) succeeded; want error")
	}
}
