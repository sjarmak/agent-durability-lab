// Package evidence validates and publishes append-only raw v2 benchmark runs.
// It has no verdict API; only the independent oracle may write verdict.json.
package evidence

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type RunIdentity struct {
	RunID      string
	Case       protocol.CaseID
	Probe      protocol.Probe
	Trial      int
	EpisodeID  string
	Seed       int64
	CohortSize int
}

type Bundle struct {
	Identity    RunIdentity
	Events      []protocol.CausalEvent
	Authority   protocol.AuthorityState
	Destination protocol.DestinationState
	Dependency  protocol.DependencyState
	Workload    protocol.WorkloadState
	Fault       protocol.FaultBoundary
	Processes   []protocol.ProcessObservation
	Native      []protocol.NativeRecord
	Input       protocol.EffectiveInput
}

func WriteRun(ctx context.Context, root string, bundle Bundle) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateBundle(root, bundle); err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", fmt.Errorf("create evidence root: %w", err)
	}
	runDir := filepath.Join(root, bundle.Identity.RunID)
	if err := os.Mkdir(runDir, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return runDir, fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, runDir)
		}
		return runDir, fmt.Errorf("create run evidence: %w", err)
	}
	if err := writeBundle(ctx, runDir, bundle); err != nil {
		return runDir, err
	}
	if err := syncDirectory(runDir); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func validateBundle(root string, bundle Bundle) error {
	identity := bundle.Identity
	if root == "" || identity.RunID == "" || identity.RunID == "." || identity.RunID == ".." ||
		identity.RunID != filepath.Base(identity.RunID) || !identity.Case.Valid() || !identity.Probe.Valid() ||
		identity.Trial < 1 || identity.EpisodeID == "" || identity.CohortSize < 1 {
		return fmt.Errorf("%w: complete path-safe run identity is required", protocol.ErrInvalidEvidence)
	}
	placeholder := strings.Repeat("0", 64)
	hashes := make(map[string]string, len(protocol.RawEvidenceFiles())-1)
	for _, name := range protocol.RawEvidenceFiles()[1:] {
		hashes[name] = placeholder
	}
	loaded := protocol.EvidenceBundle{
		Manifest: protocol.Manifest{
			ContractVersion: protocol.ContractVersion,
			RunID:           identity.RunID, Suite: identity.Case.Suite(), Case: identity.Case, Probe: identity.Probe,
			Trial: identity.Trial, EpisodeID: identity.EpisodeID, Seed: identity.Seed, CohortSize: identity.CohortSize,
			InputSHA256: placeholder, EvidenceSHA256: hashes,
		},
		Events: bundle.Events, Authority: bundle.Authority, Destination: bundle.Destination,
		Dependency: bundle.Dependency, Workload: bundle.Workload, Fault: bundle.Fault,
		Processes: bundle.Processes, Native: bundle.Native, Input: bundle.Input,
	}
	if err := loaded.Validate(); err != nil {
		return err
	}
	return nil
}

func writeBundle(ctx context.Context, runDir string, bundle Bundle) error {
	files := map[string]any{
		protocol.AuthorityStateFile:      bundle.Authority,
		protocol.DestinationStateFile:    bundle.Destination,
		protocol.DependencyStateFile:     bundle.Dependency,
		protocol.WorkloadStateFile:       bundle.Workload,
		protocol.FaultBoundaryFile:       bundle.Fault,
		protocol.NativeJournalFile:       bundle.Native,
		protocol.ProcessObservationsFile: bundle.Processes,
		protocol.EffectiveInputFile:      bundle.Input,
	}
	names := make([]string, 0, len(files)+1)
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeJSONExclusive(ctx, filepath.Join(runDir, name), files[name]); err != nil {
			return err
		}
	}
	if err := writeJSONLExclusive(ctx, filepath.Join(runDir, protocol.CausalEventsFile), bundle.Events); err != nil {
		return err
	}
	names = append(names, protocol.CausalEventsFile)
	sort.Strings(names)

	identity := bundle.Identity
	manifest := protocol.Manifest{
		ContractVersion: protocol.ContractVersion,
		RunID:           identity.RunID, Suite: identity.Case.Suite(), Case: identity.Case, Probe: identity.Probe,
		Trial: identity.Trial, EpisodeID: identity.EpisodeID, Seed: identity.Seed, CohortSize: identity.CohortSize,
		EvidenceSHA256: make(map[string]string, len(names)),
	}
	for _, name := range names {
		hash, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			return fmt.Errorf("hash %s: %w", name, err)
		}
		manifest.EvidenceSHA256[name] = hash
	}
	manifest.InputSHA256 = manifest.EvidenceSHA256[protocol.EffectiveInputFile]
	if err := manifest.Validate(); err != nil {
		return err
	}
	return writeJSONExclusive(ctx, filepath.Join(runDir, protocol.ManifestFile), manifest)
}

func writeJSONExclusive(ctx context.Context, path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeExclusive(ctx, path, append(data, '\n'))
}

func writeJSONLExclusive(ctx context.Context, path string, events []protocol.CausalEvent) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return err
		}
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			return fmt.Errorf("encode causal event: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("flush causal events: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync causal events: %w", err)
	}
	return file.Close()
}

func writeExclusive(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	return file.Close()
}

func evidenceWriteError(path string, err error) error {
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, path)
	}
	return err
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open evidence directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync evidence directory: %w", err)
	}
	return nil
}
