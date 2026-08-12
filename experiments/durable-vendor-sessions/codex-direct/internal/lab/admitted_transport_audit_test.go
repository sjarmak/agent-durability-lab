package lab

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/transport"
)

func TestAdmittedTransportsReconstructEveryVerdict(t *testing.T) {
	if os.Getenv("CODEX_DIRECT_TRANSPORT_AUDIT") != "1" {
		t.Skip("sealed transport audit runs in its own coverage process")
	}
	repositoryRoot := codexDirectRepositoryRoot(t)
	tests := []struct {
		name, transportDirectory, bundle string
		want                             EvidenceAudit
	}{
		{
			name: "hermetic-unsafe", transportDirectory: "hermetic-unsafe-20260812-v4",
			bundle: "codex-direct-hermetic-unsafe-20260812-v12",
			want: EvidenceAudit{Runs: 12, ValidPassRuns: 6, DistinguishingFailRuns: 6,
				HistoriesReplayed: 12, RawInventoriesVerified: 12, RawArtifactsVerified: 405,
				ProcessesObserved: 21, ThreadsObserved: 18, PhysicalEffects: 18,
				SourceSHA256: v12HermeticSourceSHA256()},
		},
		{
			name: "hermetic-resume", transportDirectory: "hermetic-resume-20260812-v4",
			bundle: "codex-direct-hermetic-resume-20260812-v12",
			want: EvidenceAudit{Runs: 12, ValidPassRuns: 6, DistinguishingFailRuns: 6,
				HistoriesReplayed: 12, RawInventoriesVerified: 12, RawArtifactsVerified: 429,
				ProcessesObserved: 21, ThreadsObserved: 18, PhysicalEffects: 18,
				SourceSHA256: v12HermeticSourceSHA256()},
		},
		{
			name: "hermetic-fenced", transportDirectory: "hermetic-fenced-20260812-v4",
			bundle: "codex-direct-hermetic-fenced-20260812-v12",
			want: EvidenceAudit{Runs: 27, ValidPassRuns: 27, HistoriesReplayed: 27,
				RawInventoriesVerified: 27, RawArtifactsVerified: 846, ProcessesObserved: 30,
				ThreadsObserved: 27, PhysicalEffects: 24, AttachmentsObserved: 21,
				ReplacementsObserved: 3, CancellationsObserved: 3,
				SourceSHA256: v12HermeticSourceSHA256()},
		},
		{
			name: "authenticated-unsafe", transportDirectory: "auth-unsafe-20260812-v4",
			bundle: "cdu-auth-unsafe-final-root-v12",
			want: EvidenceAudit{Runs: 12, ValidPassRuns: 6, DistinguishingFailRuns: 6,
				HistoriesReplayed: 12, RawInventoriesVerified: 12, RawArtifactsVerified: 405,
				ProcessesObserved: 21, ThreadsObserved: 18, PhysicalEffects: 18,
				SourceSHA256: v12AuthenticatedSourceSHA256()},
		},
		{
			name: "authenticated-resume", transportDirectory: "auth-resume-20260812-v4",
			bundle: "cdu-auth-resume-final-root-v12",
			want: EvidenceAudit{Runs: 12, ValidPassRuns: 6, DistinguishingFailRuns: 6,
				HistoriesReplayed: 12, RawInventoriesVerified: 12, RawArtifactsVerified: 429,
				ProcessesObserved: 21, ThreadsObserved: 18, PhysicalEffects: 18,
				SourceSHA256: v12AuthenticatedSourceSHA256()},
		},
		{
			name: "authenticated-fenced", transportDirectory: "auth-fenced-20260812-v4",
			bundle: "cdu-auth-fenced-final-root-v12",
			want: EvidenceAudit{Runs: 27, ValidPassRuns: 27, HistoriesReplayed: 27,
				RawInventoriesVerified: 27, RawArtifactsVerified: 846, ProcessesObserved: 30,
				ThreadsObserved: 27, PhysicalEffects: 24, AttachmentsObserved: 21,
				ReplacementsObserved: 3, CancellationsObserved: 3,
				SourceSHA256: v12AuthenticatedSourceSHA256()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transportRoot := filepath.Join(repositoryRoot, "experiments", "durable-vendor-sessions",
				"codex-direct", "evidence-transport", test.transportDirectory)
			restoredRoot := filepath.Join(t.TempDir(), "restored")
			if err := transport.Restore(t.Context(), transportRoot, restoredRoot); err != nil {
				t.Fatalf("restore admitted transport: %v", err)
			}
			report, err := AuditEvidence(t.Context(), filepath.Join(restoredRoot, test.bundle))
			if err != nil {
				t.Fatalf("audit admitted bundle: %v", err)
			}
			assertAdmittedAudit(t, report, test.want)
		})
	}
}

func assertAdmittedAudit(t *testing.T, got, want EvidenceAudit) {
	t.Helper()
	if !got.AllRequirementsVerified || got.CapabilityLeaks != 0 ||
		got.Runs != want.Runs || got.ValidPassRuns != want.ValidPassRuns ||
		got.DistinguishingFailRuns != want.DistinguishingFailRuns ||
		got.HistoriesReplayed != want.HistoriesReplayed ||
		got.RawInventoriesVerified != want.RawInventoriesVerified ||
		got.RawArtifactsVerified != want.RawArtifactsVerified ||
		got.ProcessesObserved != want.ProcessesObserved || got.ThreadsObserved != want.ThreadsObserved ||
		got.PhysicalEffects != want.PhysicalEffects || got.AttachmentsObserved != want.AttachmentsObserved ||
		got.ReplacementsObserved != want.ReplacementsObserved ||
		got.CancellationsObserved != want.CancellationsObserved ||
		!reflect.DeepEqual(got.SourceSHA256, want.SourceSHA256) {
		t.Fatalf("admitted audit = %+v, want population %+v with zero capability leaks", got, want)
	}
}

func v12HermeticSourceSHA256() map[string]string {
	return v12SourceSHA256(
		"7152df1d6e95db308b8181d5c3df00c187502d827c4bc5823ba05282af9489d7",
		"7152df1d6e95db308b8181d5c3df00c187502d827c4bc5823ba05282af9489d7",
	)
}

func v12AuthenticatedSourceSHA256() map[string]string {
	return v12SourceSHA256(
		"134063e133f0b4244fa3b251acf973d4fe4b4aeeacbdc135211bf480f59f1477",
		"73962b1eac648401e8d48861bda01df1d591eb8433a361103b7939c2f269dfc5",
	)
}

func v12SourceSHA256(codex, wrapper string) map[string]string {
	return map[string]string{
		"codex": codex, "wrapper": wrapper,
		"effect":   "b1b9a4b52da06165a3d412b9c93e4a301d4ef5817fb034285cc572c2f8ba022f",
		"harness":  "025e6781a1b1530c42ef2e0847542515d86ea4dca615bab2b3779e10faae92ae",
		"launcher": "2fd6b433f760fcafc052fd2d42345fc4a04a5cbc670941512eb6fe0e0860c432",
		"schema":   "d25bb1661dc68e052ad690e2271e817c6a325e11963850991cd230517db5e249",
		"worker":   "8fd2b6553387436100be072f37b785425420881b7971bbe4063b15667c153a89",
	}
}
