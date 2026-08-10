package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/sealedfs"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

const maxPairArtifactBytes = 16 << 20

const (
	PairExecutionFile = "pair-execution.json"
	PairTimingFile    = "pair-timing.jsonl"
	PairInventoryFile = "pair-inventory.json"
)

func pairEvidenceFiles() []string {
	return []string{PairTimingFile, PairExecutionFile, PairInventoryFile}
}

type PairTimingEvent struct {
	Sequence     int               `json:"sequence"`
	Topology     protocol.Topology `json:"topology"`
	Kind         string            `json:"kind"`
	TimestampUTC string            `json:"timestamp_utc"`
}

type PairInventory struct {
	ProtocolVersion string            `json:"protocol_version"`
	PairID          string            `json:"pair_id"`
	SHA256          map[string]string `json:"sha256"`
}

func PairDirectoryName(pairID string) string {
	digest := sha256.Sum256([]byte(pairID))
	return "pair-" + hex.EncodeToString(digest[:16])
}

func createPairDirectory(root, pairID string) (string, error) {
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
	directory := filepath.Join(resolved, PairDirectoryName(pairID))
	if err := os.Mkdir(directory, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: pair %s", protocol.ErrEvidenceExists, pairID)
		}
		return "", err
	}
	return directory, nil
}

func writePairEvidence(directory string, execution PairExecution) error {
	timing := make([]PairTimingEvent, 0, len(execution.Arms)*3)
	for _, arm := range execution.Arms {
		for _, event := range []struct {
			kind      string
			timestamp string
		}{{kind: "arm_ready", timestamp: arm.ReadyAtUTC}, {kind: "arm_started", timestamp: arm.StartedAtUTC}, {kind: "arm_finished", timestamp: arm.FinishedAtUTC}} {
			if event.timestamp == "" {
				continue
			}
			timing = append(timing, PairTimingEvent{Sequence: len(timing) + 1, Topology: arm.Topology, Kind: event.kind, TimestampUTC: event.timestamp})
		}
	}
	if err := sealedfs.WriteJSONLinesExclusive(filepath.Join(directory, PairTimingFile), timing); err != nil {
		return err
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(directory, PairExecutionFile), execution); err != nil {
		return err
	}
	hashes := make(map[string]string, 2)
	for _, name := range []string{PairTimingFile, PairExecutionFile} {
		digest, err := sealedfs.HashRegularFile(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		hashes[name] = digest
	}
	if err := sealedfs.WriteJSONExclusive(filepath.Join(directory, PairInventoryFile), PairInventory{
		ProtocolVersion: protocol.PublicationProtocolVersion,
		PairID:          execution.Block.PairID,
		SHA256:          hashes,
	}); err != nil {
		return err
	}
	return sealedfs.SyncDirectory(directory)
}

func LoadPair(root, directory string) (PairExecution, error) {
	resolved, err := sealedfs.ConfinedDirectory(root, directory)
	if err != nil {
		return PairExecution{}, err
	}
	directoryRoot, err := os.OpenRoot(resolved)
	if err != nil {
		return PairExecution{}, err
	}
	defer func() { _ = directoryRoot.Close() }()
	if err := sealedfs.ValidateArtifactSet(directoryRoot, pairEvidenceFiles()); err != nil {
		return PairExecution{}, err
	}
	inventoryData, err := sealedfs.ReadRegularFileOnce(directoryRoot, PairInventoryFile, maxPairArtifactBytes)
	if err != nil {
		return PairExecution{}, fmt.Errorf("%w: pair inventory: %v", protocol.ErrInvalidEvidence, err)
	}
	var inventory PairInventory
	if err := sealedfs.DecodeJSON(PairInventoryFile, inventoryData, &inventory); err != nil {
		return PairExecution{}, err
	}
	if inventory.ProtocolVersion != protocol.PublicationProtocolVersion || inventory.PairID == "" || len(inventory.SHA256) != 2 {
		return PairExecution{}, fmt.Errorf("%w: pair inventory", protocol.ErrInvalidEvidence)
	}
	artifacts := make(map[string][]byte, 2)
	for _, name := range []string{PairTimingFile, PairExecutionFile} {
		data, err := sealedfs.ReadRegularFileOnce(directoryRoot, name, maxPairArtifactBytes)
		if err != nil || sealedfs.HashBytes(data) != inventory.SHA256[name] {
			return PairExecution{}, fmt.Errorf("%w: pair hash %s", protocol.ErrInvalidEvidence, name)
		}
		artifacts[name] = data
	}
	var execution PairExecution
	if err := sealedfs.DecodeJSON(PairExecutionFile, artifacts[PairExecutionFile], &execution); err != nil {
		return PairExecution{}, err
	}
	if execution.ProtocolVersion != protocol.PublicationProtocolVersion || execution.Block.PairID != inventory.PairID ||
		(execution.Admission != protocol.AdmissionValid && execution.Admission != protocol.AdmissionInvalid) {
		return PairExecution{}, fmt.Errorf("%w: pair execution identity", protocol.ErrInvalidEvidence)
	}
	var timing []PairTimingEvent
	if err := sealedfs.DecodeJSONLines(PairTimingFile, artifacts[PairTimingFile], &timing); err != nil {
		return PairExecution{}, err
	}
	if err := validatePairTiming(timing); err != nil {
		return PairExecution{}, err
	}
	if err := sealedfs.ValidateArtifactSet(directoryRoot, pairEvidenceFiles()); err != nil {
		return PairExecution{}, err
	}
	return execution, nil
}

func validatePairTiming(timing []PairTimingEvent) error {
	var previous time.Time
	for index, event := range timing {
		parsed, err := time.Parse(time.RFC3339Nano, event.TimestampUTC)
		if err != nil || event.Sequence != index+1 || !event.Topology.Valid() ||
			!slices.Contains([]string{"arm_ready", "arm_started", "arm_finished"}, event.Kind) {
			return fmt.Errorf("%w: pair timing", protocol.ErrInvalidEvidence)
		}
		_, offset := parsed.Zone()
		if offset != 0 || index > 0 && parsed.Before(previous) {
			return fmt.Errorf("%w: pair timing UTC order", protocol.ErrInvalidEvidence)
		}
		previous = parsed
	}
	return nil
}
