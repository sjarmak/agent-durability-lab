package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
)

func ValidateReplay(replay evidence.ReplayDisposition) error {
	if replay.Captured {
		if replay.Status != evidence.ReplayPassed {
			return fmt.Errorf("captured history did not pass replay")
		}
		if replay.HistoryPath == "" || len(replay.HistoryHash) != sha256.Size*2 {
			return fmt.Errorf("captured history lacks a path or SHA-256 hash")
		}
		if _, err := hex.DecodeString(replay.HistoryHash); err != nil {
			return fmt.Errorf("captured history hash is invalid: %w", err)
		}
		return nil
	}
	if replay.Status != evidence.ReplayNotApplicable || replay.Explanation == "" {
		return fmt.Errorf("uncaptured history requires a not-applicable explanation")
	}
	if replay.HistoryPath != "" || replay.HistoryHash != "" {
		return fmt.Errorf("uncaptured history cannot name history bytes")
	}
	return nil
}
