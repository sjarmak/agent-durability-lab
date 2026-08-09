package lab

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type WorkspaceEffect struct {
	LogicalEffectID   string    `json:"logical_effect_id"`
	PhysicalAttemptID string    `json:"physical_attempt_id"`
	Payload           string    `json:"payload"`
	ActorID           string    `json:"actor_id"`
	ProcessIdentity   string    `json:"process_identity"`
	AppliedAt         time.Time `json:"applied_at"`
}

func AppendWorkspaceEffect(ctx context.Context, path string, effect WorkspaceEffect) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if path == "" || !effect.valid() {
		return errors.New("workspace path and complete effect are required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create fixture workspace: %w", err)
	}
	encoded, err := json.Marshal(effect)
	if err != nil {
		return fmt.Errorf("encode workspace effect: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open workspace effect journal: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("append workspace effect: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync workspace effect: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close workspace effect: %w", err)
	}
	return nil
}

func ReadWorkspaceEffects(path string) (effects []WorkspaceEffect, returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open workspace effect journal: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	effects = make([]WorkspaceEffect, 0)
	seen := make(map[string]bool)
	for scanner.Scan() {
		var effect WorkspaceEffect
		if err := json.Unmarshal(scanner.Bytes(), &effect); err != nil {
			return nil, fmt.Errorf("decode workspace effect: %w", err)
		}
		if !effect.valid() || seen[effect.PhysicalAttemptID] {
			return nil, errors.New("workspace effect journal contains invalid or duplicate identity")
		}
		seen[effect.PhysicalAttemptID] = true
		effects = append(effects, effect)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read workspace effect journal: %w", err)
	}
	return effects, nil
}

func (e WorkspaceEffect) valid() bool {
	return e.LogicalEffectID != "" && e.PhysicalAttemptID != "" && e.Payload != "" &&
		e.ActorID != "" && e.ProcessIdentity != "" && !e.AppliedAt.IsZero()
}
