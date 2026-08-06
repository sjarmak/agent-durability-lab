package workstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (s *Store) ExportJSONL(ctx context.Context, sessionID, destination string) error {
	snapshot, err := s.Snapshot(ctx, sessionID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("create evidence directory: %w", err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(destination), ".evidence-*.jsonl")
	if err != nil {
		return fmt.Errorf("create evidence file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set evidence permissions: %w", err)
	}
	writer := bufio.NewWriter(temporary)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for _, event := range snapshot.Events {
		if err := ctx.Err(); err != nil {
			_ = temporary.Close()
			return err
		}
		if err := encoder.Encode(event); err != nil {
			_ = temporary.Close()
			return fmt.Errorf("encode evidence event %d: %w", event.Sequence, err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("flush evidence: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync evidence: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close evidence: %w", err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish evidence: %w", err)
	}
	committed = true
	return syncDirectory(filepath.Dir(destination))
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
