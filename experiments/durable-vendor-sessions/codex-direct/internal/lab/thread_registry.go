package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var ErrCanonicalThreadConflict = errors.New("canonical Codex thread conflicts with existing registration")

type CanonicalThread struct {
	LogicalSessionID       string    `json:"logical_session_id"`
	LogicalTurnID          string    `json:"logical_turn_id"`
	ThreadID               string    `json:"thread_id"`
	FirstPhysicalAttemptID string    `json:"first_physical_attempt_id"`
	RegisteredAt           time.Time `json:"registered_at"`
}

func RegisterCanonicalThread(path string, candidate CanonicalThread) (returnErr error) {
	if !safeCommandPath(path) || !candidate.valid() {
		return errors.New("canonical Codex thread path and identity are required")
	}
	unlock, err := lockWorkspaceEffect(path + ".lock")
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unlock()) }()
	existing, err := ReadCanonicalThread(path)
	if err == nil {
		if existing.LogicalSessionID == candidate.LogicalSessionID &&
			existing.LogicalTurnID == candidate.LogicalTurnID && existing.ThreadID == candidate.ThreadID {
			return nil
		}
		return ErrCanonicalThreadConflict
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeJSONAtomicExclusive(path, candidate)
}

func WaitForCanonicalThread(ctx context.Context, path string) (CanonicalThread, error) {
	if !safeCommandPath(path) {
		return CanonicalThread{}, errors.New("canonical Codex thread path is required")
	}
	for {
		record, err := ReadCanonicalThread(path)
		if err == nil {
			return record, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return CanonicalThread{}, err
		}
		var observed CanonicalThread
		err = waitForFileBarrierChange(ctx, filepath.Dir(path), func() (bool, error) {
			candidate, readErr := ReadCanonicalThread(path)
			if errors.Is(readErr, os.ErrNotExist) {
				return false, nil
			}
			if readErr != nil {
				return false, readErr
			}
			observed = candidate
			return true, nil
		})
		if err != nil {
			return CanonicalThread{}, err
		}
		if observed.valid() {
			return observed, nil
		}
	}
}

func ReadCanonicalThread(path string) (CanonicalThread, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CanonicalThread{}, fmt.Errorf("read canonical Codex thread: %w", err)
	}
	if len(data) == 0 || len(data) > 64<<10 {
		return CanonicalThread{}, errors.New("canonical Codex thread is not bounded")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record CanonicalThread
	if err := decoder.Decode(&record); err != nil {
		return CanonicalThread{}, fmt.Errorf("decode canonical Codex thread: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return CanonicalThread{}, errors.New("canonical Codex thread contains trailing data")
	}
	if !record.valid() {
		return CanonicalThread{}, errors.New("canonical Codex thread is incomplete")
	}
	return record, nil
}

func (r CanonicalThread) valid() bool {
	return r.LogicalSessionID != "" && r.LogicalTurnID != "" && validThreadID(r.ThreadID) &&
		r.FirstPhysicalAttemptID != "" && !r.RegisteredAt.IsZero()
}
