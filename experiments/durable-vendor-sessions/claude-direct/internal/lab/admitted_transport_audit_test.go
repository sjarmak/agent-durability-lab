package lab

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/transport"
)

func TestAdmittedTransportsReconstructEveryVerdict(t *testing.T) {
	if os.Getenv("CLAUDE_DIRECT_TRANSPORT_AUDIT") != "1" {
		t.Skip("sealed transport audit runs in its own coverage process")
	}
	repositoryRoot := claudeDirectRepositoryRoot(t)
	tests := []struct {
		name      string
		transport string
		bundle    string
		audit     func(context.Context, string) error
	}{
		{
			name: "direct", transport: "evidence-transport", bundle: "claude-direct-20260808-v5",
			audit: func(ctx context.Context, root string) error {
				report, err := AuditDirectEvidence(ctx, root)
				if err == nil && (!report.AllRequirementsVerified || report.Runs != 12 || report.HistoriesReplayed != 12) {
					return fmt.Errorf("direct report is incomplete: %+v", report)
				}
				return err
			},
		},
		{
			name: "resume", transport: "resume-evidence-transport", bundle: "claude-direct-resume-20260810-v5",
			audit: func(ctx context.Context, root string) error {
				report, err := AuditResumeEvidence(ctx, root)
				if err == nil && (!report.AllRequirementsVerified || report.Runs != 12 || report.HistoriesReplayed != 12) {
					return fmt.Errorf("resume report is incomplete: %+v", report)
				}
				return err
			},
		},
		{
			name: "fenced", transport: "fenced-evidence-transport-v2", bundle: "claude-direct-fenced-hermetic-20260811-v4",
			audit: func(ctx context.Context, root string) error {
				report, err := AuditFencedEvidence(ctx, root)
				if err == nil && (!report.AllRequirementsVerified || report.Runs != 15 || report.HistoriesReplayed != 15) {
					return fmt.Errorf("fenced report is incomplete: %+v", report)
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restoreRoot := filepath.Join(t.TempDir(), "restored")
			transportRoot := filepath.Join(repositoryRoot, "experiments", "durable-vendor-sessions", "claude-direct", test.transport)
			if err := transport.Restore(t.Context(), transportRoot, restoreRoot); err != nil {
				t.Fatalf("restore admitted transport: %v", err)
			}
			if err := test.audit(t.Context(), filepath.Join(restoreRoot, test.bundle)); err != nil {
				t.Fatalf("audit admitted bundle: %v", err)
			}
		})
	}
}
