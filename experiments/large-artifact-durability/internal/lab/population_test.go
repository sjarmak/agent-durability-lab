package lab

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExpectedPopulationRunsIsCanonicalAndRejectsDuplicates(t *testing.T) {
	t.Parallel()

	runs, err := expectedPopulationRuns(
		[]Boundary{BoundaryReferencePublished, BoundaryBlobPublished},
		[]Mode{ModeUnsafe, ModeProtected}, 2,
	)
	if err != nil {
		t.Fatalf("expectedPopulationRuns: %v", err)
	}
	if len(runs) != 8 || runs[0].RunID != "blob_published-protected-trial-1" {
		t.Fatalf("canonical runs = %+v", runs)
	}
	if _, err := expectedPopulationRuns([]Boundary{BoundaryBlobPublished, BoundaryBlobPublished}, []Mode{ModeProtected}, 1); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("duplicate schedule error = %v", err)
	}
	if _, err := expectedPopulationRuns([]Boundary{"invalid"}, []Mode{ModeProtected}, 1); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("invalid schedule error = %v", err)
	}
}

func TestAuditAdmittedPopulationFromEnvironment(t *testing.T) {
	root := os.Getenv("LARGE_ARTIFACT_POPULATION_AUDIT_ROOT")
	if root == "" {
		t.Skip("set LARGE_ARTIFACT_POPULATION_AUDIT_ROOT to audit an admitted population")
	}
	index, err := AuditPopulation(root)
	if err != nil {
		t.Fatalf("AuditPopulation: %v", err)
	}
	if index.ValidRuns != 36 || index.ExpectedObservations != 36 || len(index.Runs) != 36 {
		t.Fatalf("population index = %+v", index)
	}
}

func TestPopulationRootAndInventoryRejectSymlinksAndExtras(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatalf("create population symlink: %v", err)
	}
	if err := validatePopulationRoot(alias); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("symlinked root error = %v", err)
	}
	expected, err := expectedPopulationRuns([]Boundary{BoundaryBlobPublished}, []Mode{ModeProtected}, 1)
	if err != nil {
		t.Fatalf("expectedPopulationRuns: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, expected[0].RunID), 0o750); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := validatePopulationEntries(root, expected, false); err != nil {
		t.Fatalf("valid population entries: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "extra"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write extra: %v", err)
	}
	if err := validatePopulationEntries(root, expected, false); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("extra inventory error = %v", err)
	}
}

func TestAuditPopulationRejectsReducedScheduleBeforeRunAdmission(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	index := PopulationIndex{
		Schema: populationSchema, PopulationID: filepath.Base(root), Trials: 1,
		Runs: []PopulationRun{{RunID: "blob_published-protected-trial-1", Boundary: BoundaryBlobPublished, Mode: ModeProtected}},
	}
	writeLiveJSON(t, filepath.Join(root, "population-index.json"), index)
	if _, err := AuditPopulation(root); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("reduced population error = %v, want ErrInvalidArtifact", err)
	}
}
