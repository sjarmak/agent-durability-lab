package postgresadapter

import (
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

func TestMigrationAndClaimsExposeLeaseGenerationAndSkipLocked(t *testing.T) {
	for _, fragment := range []string{"FOR UPDATE SKIP LOCKED", "generation = candidate.generation + 1", "lease_until < clock_timestamp()", "PRIMARY KEY (run_id, sequence)"} {
		if !strings.Contains(SchemaSQL, fragment) {
			t.Errorf("schema/claim protocol lacks %q", fragment)
		}
	}
}

func TestExpectedReceiptsRequireReacquisitionOnlyAtExactFault(t *testing.T) {
	plan, err := systemplan.Build(protocol.CasePoisonWorkIsolation, protocol.ProbeProtected, 1)
	if err != nil {
		t.Fatal(err)
	}
	receipts := expectedReceipts(plan)
	for index, receipt := range receipts {
		want := 1
		if plan.Steps[index].FailureOnce {
			want = 2
		}
		if receipt.Attempts != want || receipt.Generation != uint64(want) {
			t.Errorf("receipt %d = %+v, want attempts/generation %d", index, receipt, want)
		}
	}
}
