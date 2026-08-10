// Package evidence writes and verifies append-only topology benchmark runs.
package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

const maxEvidenceArtifactBytes = 64 << 20

type Inventory struct {
	ProtocolVersion string            `json:"protocol_version"`
	RunID           string            `json:"run_id"`
	SHA256          map[string]string `json:"sha256"`
}

func WriteRun(root string, bundle protocol.EvidenceBundle) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: evidence root", protocol.ErrInvalidEvidence)
	}
	if err := bundle.Manifest.Validate(); err != nil {
		return "", err
	}
	resolvedRoot, err := ensureRoot(root)
	if err != nil {
		return "", err
	}
	directory := filepath.Join(resolvedRoot, RunDirectoryName(bundle.Manifest.RunID))
	if err := os.Mkdir(directory, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: run %s", protocol.ErrEvidenceExists, bundle.Manifest.RunID)
		}
		return "", err
	}
	write := func(name string, value any) error {
		return sealedfs.WriteJSONExclusive(filepath.Join(directory, name), value)
	}
	if err := write(protocol.ManifestFile, bundle.Manifest); err != nil {
		return directory, err
	}
	if err := sealedfs.WriteJSONLinesExclusive(filepath.Join(directory, protocol.CausalEventsFile), bundle.CausalEvents); err != nil {
		return directory, err
	}
	for _, item := range []struct {
		name  string
		value any
	}{
		{name: protocol.LineageFile, value: bundle.Lineage},
		{name: protocol.AuthorityStateFile, value: bundle.Authority},
		{name: protocol.DestinationStateFile, value: bundle.Destination},
		{name: protocol.DependencyStateFile, value: bundle.Dependency},
		{name: protocol.WorkloadStateFile, value: bundle.Workload},
		{name: protocol.FaultBoundaryFile, value: bundle.FaultBoundary},
		{name: protocol.NativeHistoryFile, value: bundle.NativeHistory},
		{name: protocol.ProcessObservationsFile, value: bundle.ProcessObservations},
		{name: protocol.EffectiveInputFile, value: bundle.EffectiveInput},
		{name: protocol.VerdictFile, value: bundle.Verdict},
	} {
		if err := write(item.name, item.value); err != nil {
			return directory, err
		}
	}
	if err := sealedfs.WriteJSONLinesExclusive(filepath.Join(directory, protocol.PublicationTimingFile), bundle.Timing); err != nil {
		return directory, err
	}
	if err := write(protocol.PublicationExecutionFile, bundle.Execution); err != nil {
		return directory, err
	}
	hashes := make(map[string]string, len(protocol.EvidenceFileSetWithoutInventory()))
	for _, name := range protocol.EvidenceFileSetWithoutInventory() {
		digest, err := sealedfs.HashRegularFile(filepath.Join(directory, name))
		if err != nil {
			return directory, err
		}
		hashes[name] = digest
	}
	inventory := Inventory{ProtocolVersion: protocol.PublicationProtocolVersion, RunID: bundle.Manifest.RunID, SHA256: hashes}
	if err := write(protocol.PublicationInventoryFile, inventory); err != nil {
		return directory, err
	}
	if err := sealedfs.SyncDirectory(directory); err != nil {
		return directory, err
	}
	return directory, nil
}

func LoadRun(root, directory string) (protocol.EvidenceBundle, error) {
	resolved, err := sealedfs.ConfinedDirectory(root, directory)
	if err != nil {
		return protocol.EvidenceBundle{}, err
	}
	directoryRoot, err := os.OpenRoot(resolved)
	if err != nil {
		return protocol.EvidenceBundle{}, err
	}
	defer func() { _ = directoryRoot.Close() }()
	if err := sealedfs.ValidateArtifactSet(directoryRoot, protocol.RequiredEvidenceFiles()); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	inventoryData, err := sealedfs.ReadRegularFileOnce(directoryRoot, protocol.PublicationInventoryFile, maxEvidenceArtifactBytes)
	if err != nil {
		return protocol.EvidenceBundle{}, fmt.Errorf("%w: required file %s: %v", protocol.ErrInvalidEvidence, protocol.PublicationInventoryFile, err)
	}
	var inventory Inventory
	if err := sealedfs.DecodeJSON(protocol.PublicationInventoryFile, inventoryData, &inventory); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	wantFiles := protocol.EvidenceFileSetWithoutInventory()
	if inventory.ProtocolVersion != protocol.PublicationProtocolVersion || inventory.RunID == "" || len(inventory.SHA256) != len(wantFiles) {
		return protocol.EvidenceBundle{}, fmt.Errorf("%w: inventory identity or file count", protocol.ErrInvalidEvidence)
	}
	artifacts := make(map[string][]byte, len(wantFiles))
	for _, name := range wantFiles {
		want := inventory.SHA256[name]
		data, err := sealedfs.ReadRegularFileOnce(directoryRoot, name, maxEvidenceArtifactBytes)
		got := sealedfs.HashBytes(data)
		if err != nil || want == "" || want != got {
			return protocol.EvidenceBundle{}, fmt.Errorf("%w: inventory hash for %s", protocol.ErrInvalidEvidence, name)
		}
		artifacts[name] = data
	}
	var bundle protocol.EvidenceBundle
	readers := []struct {
		name  string
		value any
	}{
		{name: protocol.ManifestFile, value: &bundle.Manifest},
		{name: protocol.LineageFile, value: &bundle.Lineage},
		{name: protocol.AuthorityStateFile, value: &bundle.Authority},
		{name: protocol.DestinationStateFile, value: &bundle.Destination},
		{name: protocol.DependencyStateFile, value: &bundle.Dependency},
		{name: protocol.WorkloadStateFile, value: &bundle.Workload},
		{name: protocol.FaultBoundaryFile, value: &bundle.FaultBoundary},
		{name: protocol.NativeHistoryFile, value: &bundle.NativeHistory},
		{name: protocol.ProcessObservationsFile, value: &bundle.ProcessObservations},
		{name: protocol.EffectiveInputFile, value: &bundle.EffectiveInput},
		{name: protocol.VerdictFile, value: &bundle.Verdict},
		{name: protocol.PublicationExecutionFile, value: &bundle.Execution},
	}
	for _, item := range readers {
		if err := sealedfs.DecodeJSON(item.name, artifacts[item.name], item.value); err != nil {
			return protocol.EvidenceBundle{}, err
		}
	}
	if err := sealedfs.DecodeJSONLines(protocol.CausalEventsFile, artifacts[protocol.CausalEventsFile], &bundle.CausalEvents); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	if err := sealedfs.DecodeJSONLines(protocol.PublicationTimingFile, artifacts[protocol.PublicationTimingFile], &bundle.Timing); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	if bundle.Manifest.RunID != inventory.RunID {
		return protocol.EvidenceBundle{}, fmt.Errorf("%w: manifest and inventory run identity", protocol.ErrInvalidEvidence)
	}
	if err := sealedfs.ValidateArtifactSet(directoryRoot, protocol.RequiredEvidenceFiles()); err != nil {
		return protocol.EvidenceBundle{}, err
	}
	return bundle, nil
}

func RunDirectoryName(runID string) string {
	digest := sha256.Sum256([]byte(runID))
	return "run-" + hex.EncodeToString(digest[:16])
}

func ensureRoot(root string) (string, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return resolved, nil
}
