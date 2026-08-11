package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type auditedPopulation struct {
	root    string
	entries []os.DirEntry
	suite   ExperimentResult
}

type auditedRun struct {
	manifest  protocol.Manifest
	verdict   protocol.Verdict
	input     protocol.EffectiveInput
	processes []protocol.ProcessObservation
	summary   trialSummary
	inventory rawInventory
	rawRoot   string
}

func inspectAuditedPopulation(ctx context.Context, root string, wantRuns int) (auditedPopulation, error) {
	if err := ctx.Err(); err != nil {
		return auditedPopulation{}, err
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return auditedPopulation{}, fmt.Errorf("resolve evidence root: %w", err)
	}
	absoluteRoot = filepath.Clean(absoluteRoot)
	info, err := os.Lstat(absoluteRoot)
	if err != nil {
		return auditedPopulation{}, fmt.Errorf("inspect evidence root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return auditedPopulation{}, errors.New("evidence root is not a real directory")
	}
	if _, err := os.Lstat(filepath.Join(absoluteRoot, "failure.json")); err == nil {
		return auditedPopulation{}, errors.New("evidence root contains a suite failure")
	} else if !errors.Is(err, os.ErrNotExist) {
		return auditedPopulation{}, fmt.Errorf("inspect suite failure: %w", err)
	}
	entries, err := os.ReadDir(absoluteRoot)
	if err != nil {
		return auditedPopulation{}, fmt.Errorf("read evidence root: %w", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".staging-") {
			return auditedPopulation{}, fmt.Errorf("evidence root contains unfinished staging directory %q", entry.Name())
		}
	}
	suite, err := readStrictJSON[ExperimentResult](filepath.Join(absoluteRoot, "suite-summary.json"))
	if err != nil {
		return auditedPopulation{}, err
	}
	if len(suite.RunDirectories) != wantRuns {
		return auditedPopulation{}, fmt.Errorf("suite summary does not name the exact %d-run population", wantRuns)
	}
	normalizedRuns := make([]string, 0, len(suite.RunDirectories))
	seenRuns := make(map[string]bool, len(suite.RunDirectories))
	for _, recorded := range suite.RunDirectories {
		name := filepath.Base(filepath.Clean(recorded))
		if name == "." || name == string(filepath.Separator) || name == "" || seenRuns[name] {
			return auditedPopulation{}, fmt.Errorf("suite summary contains an invalid or duplicate run path %q", recorded)
		}
		runDirectory := filepath.Join(absoluteRoot, name)
		runInfo, err := os.Lstat(runDirectory)
		if err != nil {
			return auditedPopulation{}, fmt.Errorf("inspect relocated run %q: %w", name, err)
		}
		if !runInfo.IsDir() || runInfo.Mode()&os.ModeSymlink != 0 {
			return auditedPopulation{}, fmt.Errorf("relocated run %q is not a real directory", name)
		}
		seenRuns[name] = true
		normalizedRuns = append(normalizedRuns, runDirectory)
	}
	suite.EvidenceRoot = absoluteRoot
	suite.RunDirectories = normalizedRuns
	allowedEntries := map[string]bool{
		"suite-summary.json":  true,
		"temporal-server.log": true,
		"temporal.db":         true,
	}
	for name := range seenRuns {
		allowedEntries[name] = true
	}
	if len(entries) != len(allowedEntries) {
		return auditedPopulation{}, errors.New("evidence root does not contain the exact sealed population")
	}
	for _, entry := range entries {
		if !allowedEntries[entry.Name()] || entry.Type()&os.ModeSymlink != 0 {
			return auditedPopulation{}, fmt.Errorf("unexpected or symlinked evidence-root entry %q", entry.Name())
		}
		if seenRuns[entry.Name()] {
			if !entry.IsDir() {
				return auditedPopulation{}, fmt.Errorf("run entry %q is not a directory", entry.Name())
			}
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return auditedPopulation{}, fmt.Errorf("suite artifact %q is not a regular file", entry.Name())
		}
	}
	return auditedPopulation{root: absoluteRoot, entries: entries, suite: suite}, nil
}

func readAuditedRun(ctx context.Context, runDirectory string) (auditedRun, error) {
	manifest, err := readStrictJSON[protocol.Manifest](filepath.Join(runDirectory, protocol.ManifestFile))
	if err != nil {
		return auditedRun{}, err
	}
	if manifest.RunID != filepath.Base(runDirectory) {
		return auditedRun{}, fmt.Errorf("manifest run identity differs from directory %q", runDirectory)
	}
	storedVerdict, err := readStrictJSON[protocol.Verdict](filepath.Join(runDirectory, protocol.VerdictFile))
	if err != nil {
		return auditedRun{}, err
	}
	recomputed := oracle.Evaluate(ctx, runDirectory)
	if !reflect.DeepEqual(storedVerdict, recomputed) {
		return auditedRun{}, fmt.Errorf("stored verdict differs from disk-only reconstruction for %s", manifest.RunID)
	}
	input, err := readStrictJSON[protocol.EffectiveInput](filepath.Join(runDirectory, protocol.EffectiveInputFile))
	if err != nil {
		return auditedRun{}, err
	}
	processes, err := readStrictJSON[[]protocol.ProcessObservation](filepath.Join(runDirectory, protocol.ProcessObservationsFile))
	if err != nil {
		return auditedRun{}, err
	}
	rawRoot := filepath.Join(runDirectory, "raw")
	summary, err := readStrictJSON[trialSummary](filepath.Join(rawRoot, "trial-summary.json"))
	if err != nil {
		return auditedRun{}, err
	}
	if err := verifyRawInventory(rawRoot, input.Settings["raw_inventory_sha256"]); err != nil {
		return auditedRun{}, fmt.Errorf("verify raw inventory for %s: %w", manifest.RunID, err)
	}
	inventory, err := readStrictJSON[rawInventory](filepath.Join(rawRoot, rawInventoryFile))
	if err != nil {
		return auditedRun{}, err
	}
	if err := auditPreservedHistory(filepath.Join(rawRoot, "temporal-history.json"), manifest, summary); err != nil {
		return auditedRun{}, fmt.Errorf("replay %s: %w", manifest.RunID, err)
	}
	return auditedRun{
		manifest: manifest, verdict: storedVerdict, input: input, processes: processes, summary: summary,
		inventory: inventory, rawRoot: rawRoot,
	}, nil
}

func processIdentitiesByActor(observations []protocol.ProcessObservation) (map[string]string, error) {
	identities := make(map[string]string, len(observations))
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			return nil, err
		}
		if _, exists := identities[observation.ActorID]; exists {
			return nil, fmt.Errorf("duplicate process observation for actor %q", observation.ActorID)
		}
		identities[observation.ActorID] = observation.ProcessIdentity
	}
	return identities, nil
}

func readPopulationRun(ctx context.Context, population auditedPopulation, seen map[string]bool, runDirectory string) (auditedRun, error) {
	if err := ctx.Err(); err != nil {
		return auditedRun{}, err
	}
	runDirectory = filepath.Clean(runDirectory)
	if filepath.Dir(runDirectory) != population.root || filepath.Base(runDirectory) == "." || seen[runDirectory] {
		return auditedRun{}, fmt.Errorf("suite contains duplicate or unconfined run directory %q", runDirectory)
	}
	info, err := os.Lstat(runDirectory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return auditedRun{}, fmt.Errorf("run directory is absent, non-directory, or symlinked: %q", runDirectory)
	}
	seen[runDirectory] = true
	return readAuditedRun(ctx, runDirectory)
}

func validateListedRunDirectories(root string, seen map[string]bool, entries []os.DirEntry) error {
	for _, entry := range entries {
		if entry.IsDir() && !seen[filepath.Join(root, entry.Name())] {
			return fmt.Errorf("unlisted run directory %q", entry.Name())
		}
	}
	return nil
}

func readAuditedEffectRequest(path string) (ControlledEffectInput, error) {
	input, err := readStrictJSON[ControlledEffectInput](path)
	if err != nil {
		return ControlledEffectInput{}, err
	}
	if !input.valid() {
		return ControlledEffectInput{}, errors.New("controlled effect request is incomplete")
	}
	return input, nil
}
