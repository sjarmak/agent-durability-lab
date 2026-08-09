package publication

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type PairInventory struct {
	ProtocolVersion string            `json:"protocol_version"`
	PairID          string            `json:"pair_id"`
	SHA256          map[string]string `json:"sha256"`
}

func createPairDirectory(root, pairID string) (string, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", err
	}
	directory := filepath.Join(root, PairDirectoryName(pairID))
	if err := os.Mkdir(directory, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: pair %s", protocol.ErrEvidenceExists, pairID)
		}
		return "", err
	}
	return directory, nil
}

func writePairEvidence(directory string, execution PairExecution) error {
	if err := writeTiming(filepath.Join(directory, PublicationTimingFile), execution.Systems); err != nil {
		return err
	}
	if err := writeJSONExclusive(filepath.Join(directory, PublicationExecutionFile), execution); err != nil {
		return err
	}
	hashes := make(map[string]string, 2)
	for _, name := range []string{PublicationTimingFile, PublicationExecutionFile} {
		hash, err := fileHash(filepath.Join(directory, name))
		if err != nil {
			return err
		}
		hashes[name] = hash
	}
	inventory := PairInventory{ProtocolVersion: ProtocolVersion, PairID: execution.PairID, SHA256: hashes}
	return writeJSONExclusive(filepath.Join(directory, PublicationInventoryFile), inventory)
}

func writeTiming(path string, systems []SystemRun) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	buffer := bufio.NewWriter(file)
	encoder := json.NewEncoder(buffer)
	for _, system := range systems {
		for _, event := range system.Timing {
			if err := encoder.Encode(event); err != nil {
				return err
			}
		}
	}
	if err := buffer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func writeJSONExclusive(path string, value any) (returnErr error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return file.Sync()
}
