package lab

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

const populationSchema = "large-artifact-durability-population-v1"

type PopulationRun struct {
	RunID              string   `json:"run_id"`
	Boundary           Boundary `json:"boundary"`
	Mode               Mode     `json:"mode"`
	ManifestSHA256     string   `json:"manifest_sha256"`
	InvariantSatisfied bool     `json:"invariant_satisfied"`
}

type PopulationIndex struct {
	Schema               string          `json:"schema"`
	PopulationID         string          `json:"population_id"`
	Trials               int             `json:"trials"`
	Runs                 []PopulationRun `json:"runs"`
	ValidRuns            int             `json:"valid_runs"`
	ExpectedObservations int             `json:"expected_observations"`
	SatisfiedInvariants  int             `json:"satisfied_invariants"`
}

func PreservePopulationIndex(root string, boundaries []Boundary, modes []Mode, trials int) (PopulationIndex, error) {
	if err := validatePopulationRoot(root); err != nil {
		return PopulationIndex{}, err
	}
	expected, err := expectedPopulationRuns(boundaries, modes, trials)
	if err != nil {
		return PopulationIndex{}, err
	}
	if err := validatePopulationEntries(root, expected, false); err != nil {
		return PopulationIndex{}, err
	}
	index, err := reconstructPopulation(root, expected, trials)
	if err != nil {
		return PopulationIndex{}, err
	}
	if err := writeJSONExclusive(filepath.Join(root, "population-index.json"), index); err != nil {
		return PopulationIndex{}, err
	}
	return index, nil
}

func AuditPopulation(root string) (PopulationIndex, error) {
	if err := validatePopulationRoot(root); err != nil {
		return PopulationIndex{}, err
	}
	data, err := readBoundedRegular(filepath.Join(root, "population-index.json"), maxEvidenceJSONBytes)
	if err != nil {
		return PopulationIndex{}, err
	}
	var stored PopulationIndex
	if err := decodeStrictJSON(data, &stored); err != nil {
		return PopulationIndex{}, err
	}
	if stored.Schema != populationSchema || stored.PopulationID != filepath.Base(root) || stored.Trials != 3 {
		return PopulationIndex{}, fmt.Errorf("%w: population identity is invalid", ErrInvalidArtifact)
	}
	expected, err := expectedPopulationRuns(admittedPopulationBoundaries(), admittedPopulationModes(), stored.Trials)
	if err != nil {
		return PopulationIndex{}, err
	}
	if err := validatePopulationEntries(root, expected, true); err != nil {
		return PopulationIndex{}, err
	}
	reconstructed, err := reconstructPopulation(root, expected, stored.Trials)
	if err != nil {
		return PopulationIndex{}, err
	}
	if !reflect.DeepEqual(reconstructed, stored) {
		return PopulationIndex{}, fmt.Errorf("%w: population index differs from audited runs", ErrArtifactConflict)
	}
	return reconstructed, nil
}

func admittedPopulationBoundaries() []Boundary {
	return []Boundary{
		BoundaryBlobPublished, BoundaryReferenceCreated, BoundaryReferencePublished,
		BoundaryActivityCompleted, BoundaryAcknowledgementPublished, BoundaryExternalStorageStored,
	}
}

func admittedPopulationModes() []Mode { return []Mode{ModeUnsafe, ModeProtected} }

func reconstructPopulation(root string, expected []PopulationRun, trials int) (PopulationIndex, error) {
	index := PopulationIndex{Schema: populationSchema, PopulationID: filepath.Base(root), Trials: trials}
	for _, run := range expected {
		verdict, err := AuditRun(filepath.Join(root, run.RunID))
		if err != nil {
			return PopulationIndex{}, fmt.Errorf("audit %s: %w", run.RunID, err)
		}
		manifest, err := readBoundedRegular(filepath.Join(root, run.RunID, "manifest.json"), maxEvidenceJSONBytes)
		if err != nil {
			return PopulationIndex{}, err
		}
		run.ManifestSHA256 = digestBytes(manifest)
		run.InvariantSatisfied = verdict.InvariantSatisfied
		index.Runs = append(index.Runs, run)
		if verdict.RunValid {
			index.ValidRuns++
		}
		if verdict.ExpectedObservation {
			index.ExpectedObservations++
		}
		if verdict.InvariantSatisfied {
			index.SatisfiedInvariants++
		}
	}
	return index, nil
}

func expectedPopulationRuns(boundaries []Boundary, modes []Mode, trials int) ([]PopulationRun, error) {
	if len(boundaries) == 0 || len(modes) == 0 || trials < 1 || trials > 3 {
		return nil, fmt.Errorf("%w: population schedule is invalid", ErrInvalidArtifact)
	}
	seen := make(map[string]struct{})
	boundaries = append([]Boundary(nil), boundaries...)
	modes = append([]Mode(nil), modes...)
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })
	sort.Slice(modes, func(i, j int) bool { return modes[i] < modes[j] })
	runs := make([]PopulationRun, 0, len(boundaries)*len(modes)*trials)
	for _, boundary := range boundaries {
		for _, mode := range modes {
			if !boundary.Valid() || !mode.valid() {
				return nil, fmt.Errorf("%w: population boundary or mode is invalid", ErrInvalidArtifact)
			}
			for trial := 1; trial <= trials; trial++ {
				runID := fmt.Sprintf("%s-%s-trial-%d", boundary, mode, trial)
				if _, duplicate := seen[runID]; duplicate {
					return nil, fmt.Errorf("%w: duplicate population run %q", ErrInvalidArtifact, runID)
				}
				seen[runID] = struct{}{}
				runs = append(runs, PopulationRun{RunID: runID, Boundary: boundary, Mode: mode})
			}
		}
	}
	return runs, nil
}

func validatePopulationRoot(root string) error {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != filepath.Clean(absolute) {
		return fmt.Errorf("%w: population root traverses a symlink", ErrInvalidArtifact)
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: population root is not a real directory", ErrInvalidArtifact)
	}
	return nil
}

func validatePopulationEntries(root string, expected []PopulationRun, withIndex bool) error {
	want := make(map[string]bool, len(expected)+1)
	for _, run := range expected {
		want[run.RunID] = true
	}
	if withIndex {
		want["population-index.json"] = false
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != len(want) {
		return fmt.Errorf("%w: population inventory cardinality differs", ErrInvalidArtifact)
	}
	for _, entry := range entries {
		wantDirectory, found := want[entry.Name()]
		info, infoErr := entry.Info()
		if !found || infoErr != nil || info.IsDir() != wantDirectory || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: unexpected population entry %q", ErrInvalidArtifact, entry.Name())
		}
	}
	return nil
}
