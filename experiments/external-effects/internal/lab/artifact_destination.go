package lab

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func prepareArtifactDestination(root string) error {
	if root == "" {
		return fmt.Errorf("%w: artifact path is required", ErrInvalidEffect)
	}
	for _, directory := range []string{filepath.Join(root, "blobs"), filepath.Join(root, "refs")} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return fmt.Errorf("create artifact directory: %w", err)
		}
	}
	return nil
}

func applyArtifactEffect(root string, request EffectRequest) (EffectResult, error) {
	digest := sha256.Sum256([]byte(request.Payload))
	digestText := hex.EncodeToString(digest[:])
	name := digestText + ".blob"
	if request.Mode == ModeUnsafe {
		name = request.EffectID + "-attempt-" + strconv.Itoa(int(request.Attempt)) + "-" + digestText + ".blob"
	}
	blobPath := filepath.Join(root, "blobs", name)
	receipt := "artifact:" + name
	if request.Mode == ModeProtected {
		referencePath := filepath.Join(root, "refs", request.EffectID+".ref")
		existingReference, err := os.ReadFile(referencePath)
		if err == nil {
			if strings.TrimSpace(string(existingReference)) != receipt {
				return EffectResult{}, fmt.Errorf("artifact reference %q has conflicting content", request.EffectID)
			}
			created, err := writeExclusiveOrValidate(blobPath, []byte(request.Payload))
			if err != nil {
				return EffectResult{}, err
			}
			outcome := OutcomeDeduplicated
			if created {
				outcome = OutcomeReconciled
			}
			return EffectResult{Receipt: receipt, Outcome: outcome}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return EffectResult{}, fmt.Errorf("read artifact reference: %w", err)
		}
	}
	created, err := writeExclusiveOrValidate(blobPath, []byte(request.Payload))
	if err != nil {
		return EffectResult{}, err
	}
	if request.Mode == ModeProtected {
		referencePath := filepath.Join(root, "refs", request.EffectID+".ref")
		if _, err := writeExclusiveOrValidate(referencePath, []byte(receipt+"\n")); err != nil {
			return EffectResult{}, err
		}
		if !created {
			return EffectResult{Receipt: receipt, Outcome: OutcomeDeduplicated}, nil
		}
	}
	return EffectResult{Receipt: receipt, Outcome: OutcomeApplied}, nil
}

func snapshotArtifactDestination(root string) (DestinationState, error) {
	entries, err := os.ReadDir(filepath.Join(root, "blobs"))
	if err != nil {
		return DestinationState{}, fmt.Errorf("read artifact blobs: %w", err)
	}
	state := DestinationState{PhysicalEffects: make([]PhysicalEffect, 0, len(entries))}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".blob") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return DestinationState{}, fmt.Errorf("inspect artifact blob: %w", err)
		}
		logicalID := ""
		attempt := int32(0)
		parts := strings.Split(entry.Name(), "-")
		if len(parts) >= 4 && parts[len(parts)-3] == "attempt" {
			logicalID = strings.Join(parts[:len(parts)-3], "-")
			parsed, parseErr := strconv.ParseInt(parts[len(parts)-2], 10, 32)
			if parseErr == nil {
				attempt = int32(parsed)
			}
		} else {
			references, err := os.ReadDir(filepath.Join(root, "refs"))
			if err != nil {
				return DestinationState{}, fmt.Errorf("read artifact references: %w", err)
			}
			for _, reference := range references {
				data, err := os.ReadFile(filepath.Join(root, "refs", reference.Name()))
				if err != nil {
					return DestinationState{}, fmt.Errorf("read artifact reference: %w", err)
				}
				if strings.TrimSpace(string(data)) == "artifact:"+entry.Name() {
					logicalID = strings.TrimSuffix(reference.Name(), ".ref")
					break
				}
			}
		}
		state.PhysicalEffects = append(state.PhysicalEffects, PhysicalEffect{
			PhysicalID: entry.Name(), LogicalID: logicalID, Receipt: "artifact:" + entry.Name(),
			AppliedAt: info.ModTime().UTC(), Attempt: attempt, Kind: DestinationArtifact,
		})
	}
	return state, nil
}

func writeExclusiveOrValidate(path string, content []byte) (bool, error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".artifact-*")
	if err != nil {
		return false, fmt.Errorf("create temporary %s: %w", path, err)
	}
	temporary := file.Name()
	cleanup := func() error {
		if err := os.Remove(temporary); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove temporary %s: %w", temporary, err)
		}
		return nil
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		_ = cleanup()
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = cleanup()
		return false, fmt.Errorf("sync %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		_ = cleanup()
		return false, fmt.Errorf("close %s: %w", path, err)
	}
	if err := os.Link(temporary, path); errors.Is(err, os.ErrExist) {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return false, cleanupErr
		}
		existing, readErr := os.ReadFile(path)
		if readErr != nil {
			return false, fmt.Errorf("read existing %s: %w", path, readErr)
		}
		if string(existing) != string(content) {
			return false, fmt.Errorf("existing %s has conflicting content", path)
		}
		return false, nil
	} else if err != nil {
		_ = cleanup()
		return false, fmt.Errorf("publish %s: %w", path, err)
	}
	if err := cleanup(); err != nil {
		return false, err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, fmt.Errorf("open parent of %s: %w", path, err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return false, fmt.Errorf("sync parent of %s: %w", path, err)
	}
	if err := directory.Close(); err != nil {
		return false, fmt.Errorf("close parent of %s: %w", path, err)
	}
	return true, nil
}
