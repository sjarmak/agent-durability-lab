package postgresadapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

func TestLivePostgresQueueReacquiresExpiredLeaseAndExportsJournal(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is required for live PostgreSQL adapter test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := Open(ctx, Config{DSN: dsn, AdapterVersion: "source-sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := systemplan.Build(protocol.CaseBackpressureOverload, protocol.ProbeProtected, 1)
	execution, err := session.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if execution.SystemID != "postgresql-queue" || len(execution.Native) == 0 || execution.Settings["claim"] != "for-update-skip-locked" {
		t.Fatalf("execution = %+v", execution)
	}
}

func TestLivePostgresQueueRejectsCompletionFromExpiredGeneration(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is required for live PostgreSQL adapter test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := Open(ctx, Config{DSN: dsn, AdapterVersion: "source-sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := systemplan.Build(protocol.CaseABAReacquisition, protocol.ProbeProtected, 91)
	runID := "stale-completion-" + randomSuffix()
	if err := session.insertPlan(ctx, runID, plan); err != nil {
		t.Fatal(err)
	}
	oldReceipt, err := session.claim(ctx, runID, 1, "old-worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.expire(ctx, runID, oldReceipt, "old-worker"); err != nil {
		t.Fatal(err)
	}
	currentReceipt, err := session.claim(ctx, runID, 1, "current-worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.complete(ctx, runID, oldReceipt, "old-worker"); err == nil {
		t.Fatal("expired generation completed the current lease")
	}
	if err := session.complete(ctx, runID, currentReceipt, "current-worker"); err != nil {
		t.Fatalf("current generation completion: %v", err)
	}
}
