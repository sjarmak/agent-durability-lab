package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func prepareGitDestination(ctx context.Context, repositoryPath string) error {
	if repositoryPath == "" {
		return fmt.Errorf("%w: Git path is required", ErrInvalidEffect)
	}
	if err := os.MkdirAll(repositoryPath, 0o750); err != nil {
		return fmt.Errorf("create Git destination: %w", err)
	}
	if _, err := os.Stat(filepath.Join(repositoryPath, ".git")); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect Git destination: %w", err)
	}
	if _, err := runGit(ctx, repositoryPath, "init", "--quiet", "--initial-branch=main"); err != nil {
		return err
	}
	if _, err := runGit(ctx, repositoryPath, "config", "user.name", "Agent Durability Lab"); err != nil {
		return err
	}
	if _, err := runGit(ctx, repositoryPath, "config", "user.email", "lab@example.invalid"); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(repositoryPath, "README.md"), []byte("# Effect destination\n"), 0o600); err != nil {
		return fmt.Errorf("write Git base file: %w", err)
	}
	if _, err := runGit(ctx, repositoryPath, "add", "--", "README.md"); err != nil {
		return err
	}
	_, err := runGit(ctx, repositoryPath, "commit", "--quiet", "-m", "base")
	return err
}

func applyGitEffect(ctx context.Context, repositoryPath string, request EffectRequest) (EffectResult, error) {
	relativePath := filepath.ToSlash(filepath.Join("effects", request.EffectID+".txt"))
	absolutePath := filepath.Join(repositoryPath, filepath.FromSlash(relativePath))
	if request.Mode == ModeProtected {
		existing, err := os.ReadFile(absolutePath)
		if err == nil {
			if string(existing) != request.Payload {
				return EffectResult{}, fmt.Errorf("git marker %q has conflicting content", relativePath)
			}
			revision, err := runGit(ctx, repositoryPath, "log", "-1", "--format=%H", "--", relativePath)
			if err != nil {
				return EffectResult{}, err
			}
			if revision == "" {
				return EffectResult{}, fmt.Errorf("git marker %q is not committed", relativePath)
			}
			return EffectResult{Receipt: "git:" + revision, Outcome: OutcomeReconciled}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return EffectResult{}, fmt.Errorf("read Git marker: %w", err)
		}
	} else {
		relativePath = filepath.ToSlash(filepath.Join(
			"effects", request.EffectID+"-attempt-"+strconv.Itoa(int(request.Attempt))+".txt",
		))
		absolutePath = filepath.Join(repositoryPath, filepath.FromSlash(relativePath))
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o750); err != nil {
		return EffectResult{}, fmt.Errorf("create Git effect directory: %w", err)
	}
	if err := os.WriteFile(absolutePath, []byte(request.Payload), 0o600); err != nil {
		return EffectResult{}, fmt.Errorf("write Git effect: %w", err)
	}
	if _, err := runGit(ctx, repositoryPath, "add", "--", relativePath); err != nil {
		return EffectResult{}, err
	}
	message := "effect:" + request.EffectID + ":attempt:" + strconv.Itoa(int(request.Attempt))
	if _, err := runGit(ctx, repositoryPath, "commit", "--quiet", "-m", message); err != nil {
		return EffectResult{}, err
	}
	revision, err := runGit(ctx, repositoryPath, "rev-parse", "HEAD")
	if err != nil {
		return EffectResult{}, err
	}
	return EffectResult{Receipt: "git:" + revision, Outcome: OutcomeApplied}, nil
}

func snapshotGitDestination(ctx context.Context, repositoryPath string) (DestinationState, error) {
	output, err := runGit(ctx, repositoryPath, "log", "--reverse", "--format=%H%x09%cI%x09%s")
	if err != nil {
		return DestinationState{}, err
	}
	state := DestinationState{PhysicalEffects: make([]PhysicalEffect, 0)}
	for _, line := range strings.Split(output, "\n") {
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 || !strings.HasPrefix(fields[2], "effect:") {
			continue
		}
		parts := strings.Split(fields[2], ":")
		if len(parts) < 2 {
			return DestinationState{}, fmt.Errorf("parse Git effect commit %q", fields[2])
		}
		appliedAt, err := time.Parse(time.RFC3339, fields[1])
		if err != nil {
			return DestinationState{}, fmt.Errorf("parse Git commit time: %w", err)
		}
		attempt := int32(0)
		if len(parts) == 4 {
			parsed, err := strconv.ParseInt(parts[3], 10, 32)
			if err != nil {
				return DestinationState{}, fmt.Errorf("parse Git effect attempt: %w", err)
			}
			attempt = int32(parsed)
		}
		state.PhysicalEffects = append(state.PhysicalEffects, PhysicalEffect{
			PhysicalID: fields[0], LogicalID: parts[1], Receipt: "git:" + fields[0],
			AppliedAt: appliedAt, Attempt: attempt, Kind: DestinationGit,
		})
	}
	return state, nil
}

func runGit(ctx context.Context, repositoryPath string, arguments ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", repositoryPath}, arguments...)...)
	output, err := command.CombinedOutput()
	trimmed := strings.TrimSpace(string(output))
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err, trimmed)
	}
	return trimmed, nil
}
