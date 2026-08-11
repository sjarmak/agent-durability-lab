package lab

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	evidencetransport "github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/transport"
)

func TestAuditDirectEvidenceRestoredFromRepositoryTransport(t *testing.T) {
	repositoryRoot := claudeDirectRepositoryRoot(t)
	transportRoot := filepath.Join(repositoryRoot,
		"experiments", "durable-vendor-sessions", "claude-direct", "evidence-transport")
	temporaryRoot := t.TempDir()
	restoredRoot := filepath.Join(temporaryRoot, "evidence")
	if err := evidencetransport.Restore(context.Background(), transportRoot, restoredRoot); err != nil {
		t.Fatalf("restore direct evidence: %v", err)
	}

	report, err := AuditDirectEvidence(context.Background(), filepath.Join(restoredRoot, "claude-direct-20260808-v5"))
	if err != nil {
		t.Fatalf("audit restored direct evidence: %v", err)
	}
	if !report.AllRequirementsVerified || report.Runs != 12 || report.UnfaultedRuns != 3 ||
		report.UnsafeRuns != 9 || report.ValidPassVerdicts != 3 || report.ValidFailVerdicts != 9 ||
		report.ProcessesObserved != 21 || report.PhysicalEffects != 21 || report.WorkspaceEffects != 21 ||
		report.AcceptedOutcomes != 12 || report.ProviderSessions != 21 || report.HistoriesReplayed != 12 ||
		report.RawArtifactsVerified != 345 {
		t.Fatalf("direct evidence audit = %+v", report)
	}

	directOutput := filepath.Join(temporaryRoot, "direct-audit.json")
	if err := WriteDirectEvidenceAudit(directOutput, report); err != nil {
		t.Fatalf("write direct audit: %v", err)
	}
	if info, err := os.Stat(directOutput); err != nil || info.Size() == 0 {
		t.Fatalf("direct audit output: info=%v err=%v", info, err)
	}

	resumeOutput := filepath.Join(temporaryRoot, "resume-audit.json")
	resumeReport := ResumeEvidenceAudit{
		EvidenceRoot: report.EvidenceRoot, AllRequirementsVerified: true,
	}
	if err := WriteResumeEvidenceAudit(resumeOutput, resumeReport); err != nil {
		t.Fatalf("write resume audit: %v", err)
	}
}
