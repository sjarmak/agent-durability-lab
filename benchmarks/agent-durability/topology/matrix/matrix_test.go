package matrix

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

func TestPersistLiveArmResultRetainsBundleWhenCleanupFails(t *testing.T) {
	registration := loadRegistration(t)
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePublication)
	if err != nil {
		t.Fatal(err)
	}
	block := schedule.Blocks[0]
	bundle := testfixture.Bundle(block, block.TopologyOrder[0])
	cleanupErr := errors.New("injected cleanup failure")
	root := t.TempDir()
	result, err := persistLiveArmResult(root, runner.RunRequest{RunID: bundle.Manifest.RunID}, semantics.EpisodeResult{
		Bundle: bundle,
	}, cleanupErr)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("persist error = %v, want cleanup failure", err)
	}
	if result.RunID != bundle.Manifest.RunID || result.EvidenceDirectory == "" {
		t.Fatalf("persisted result = %+v", result)
	}
	if _, _, err := oracle.VerifyRun(root, result.EvidenceDirectory); err != nil {
		t.Fatalf("verify retained failure bundle: %v", err)
	}
}

func TestAuditFrozenPublicationScheduleExactArithmetic(t *testing.T) {
	registration := loadRegistration(t)
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePublication)
	if err != nil {
		t.Fatal(err)
	}
	audit, selected, err := AuditSchedule(registration, schedule)
	if err != nil {
		t.Fatal(err)
	}
	if audit.Strata != 88 || audit.ScheduleBlocks != 3520 || audit.PrimaryPairs != 2640 || audit.ReservePairs != 880 ||
		audit.PrimaryArmExecutions != 5280 || len(selected) != 88 || audit.PrimaryDirectFirst != 1320 ||
		audit.PrimaryChildFirst != 1320 || audit.ReserveDirectFirst != 440 || audit.ReserveChildFirst != 440 {
		t.Fatalf("schedule audit = %+v selected=%d", audit, len(selected))
	}
}

func TestSelectLiveSentinelsCoversEveryUnsafeBoundaryAndBothSuiteBaselines(t *testing.T) {
	registration := loadRegistration(t)
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePublication)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := SelectLiveSentinels(schedule)
	if err != nil {
		t.Fatal(err)
	}
	unsafe, passing := 0, 0
	suites := map[protocol.SuiteID]bool{}
	seen := map[string]bool{}
	for _, block := range blocks {
		if block.Slot != 2 || block.Reserve || seen[block.Stratum.ID] {
			t.Fatalf("invalid or duplicate sentinel block: %+v", block)
		}
		seen[block.Stratum.ID] = true
		suites[block.Stratum.Case.Suite()] = true
		if block.Stratum.Probe == protocol.ProbeUnsafe {
			unsafe++
		} else {
			passing++
		}
	}
	if len(blocks) != 23 || unsafe != 19 || passing != 4 || !suites[protocol.SuiteOrchestrationSemantics] || !suites[protocol.SuiteRecoveryDynamics] {
		t.Fatalf("sentinel selection: blocks=%d unsafe=%d passing=%d suites=%v", len(blocks), unsafe, passing, suites)
	}
}

func TestRunFixtureConformanceAdmitsAllStrataAndRetainsInvalidControls(t *testing.T) {
	root := filepath.Join(t.TempDir(), "matrix-fixtures")
	report, err := RunFixtureConformance(context.Background(), Config{
		Root: root, PreregistrationPath: preregistrationPath(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Kind != FixtureConformanceKind || !report.PublicationExcluded || report.SelectedStrata != 88 ||
		report.ValidPairs != 88 || report.ValidArms != 176 || report.UnsafeArmsDistinguished != report.UnsafeArms ||
		report.ProtectedOrUnfaultedArmsPassed != report.ProtectedOrUnfaultedArms || report.InvalidControlsRejected != 4 ||
		len(report.HarnessBinarySHA256) != 64 || report.AgentBinarySHA256 != "" || report.TemporalBinarySHA256 != "" {
		t.Fatalf("fixture report = %+v", report)
	}
	audited, err := Audit(root)
	if err != nil {
		t.Fatal(err)
	}
	if audited != report {
		t.Fatalf("audited report differs: got %+v want %+v", audited, report)
	}
	if err := os.WriteFile(filepath.Join(root, "unsealed-extra.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Audit(root); !errors.Is(err, protocol.ErrInvalidEvidence) {
		t.Fatalf("unsealed extra artifact error = %v", err)
	}
}

func loadRegistration(t *testing.T) protocol.Preregistration {
	t.Helper()
	registration, err := protocol.LoadPreregistration(preregistrationPath())
	if err != nil {
		t.Fatal(err)
	}
	return registration
}

func preregistrationPath() string {
	return filepath.Join("..", "..", "topology-preregistration-v1.json")
}
