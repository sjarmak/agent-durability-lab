package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var ErrInvalidEffect = errors.New("invalid external effect request")

func prepareDestination(ctx context.Context, destination Destination, configuration DestinationConfig) error {
	switch destination {
	case DestinationIdempotentAPI, DestinationNonIdempotentAPI, DestinationMessage:
		if configuration.HTTPURL == "" {
			return fmt.Errorf("%w: HTTP URL is required", ErrInvalidEffect)
		}
		return nil
	case DestinationDatabase:
		if configuration.DatabasePath == "" {
			return fmt.Errorf("%w: database path is required", ErrInvalidEffect)
		}
		return os.MkdirAll(filepath.Dir(configuration.DatabasePath), 0o750)
	case DestinationGit:
		return prepareGitDestination(ctx, configuration.GitPath)
	case DestinationArtifact:
		return prepareArtifactDestination(configuration.ArtifactPath)
	default:
		return fmt.Errorf("%w: unknown destination %q", ErrInvalidEffect, destination)
	}
}

func applyEffect(
	ctx context.Context,
	destination Destination,
	configuration DestinationConfig,
	request EffectRequest,
) (EffectResult, error) {
	if err := validateEffectRequest(destination, request); err != nil {
		return EffectResult{}, err
	}
	switch destination {
	case DestinationIdempotentAPI, DestinationNonIdempotentAPI, DestinationMessage:
		return applyHTTPEffect(ctx, destination, configuration.HTTPURL, request)
	case DestinationDatabase:
		return applyDatabaseEffect(configuration.DatabasePath, request)
	case DestinationGit:
		return applyGitEffect(ctx, configuration.GitPath, request)
	case DestinationArtifact:
		return applyArtifactEffect(configuration.ArtifactPath, request)
	default:
		return EffectResult{}, fmt.Errorf("%w: unknown destination %q", ErrInvalidEffect, destination)
	}
}

func snapshotDestination(
	ctx context.Context,
	destination Destination,
	configuration DestinationConfig,
) (DestinationState, error) {
	switch destination {
	case DestinationIdempotentAPI, DestinationNonIdempotentAPI, DestinationMessage:
		return snapshotHTTPDestination(ctx, configuration.HTTPURL, destination)
	case DestinationDatabase:
		return snapshotDatabaseDestination(configuration.DatabasePath)
	case DestinationGit:
		return snapshotGitDestination(ctx, configuration.GitPath)
	case DestinationArtifact:
		return snapshotArtifactDestination(configuration.ArtifactPath)
	default:
		return DestinationState{}, fmt.Errorf("%w: unknown destination %q", ErrInvalidEffect, destination)
	}
}

func validateEffectRequest(destination Destination, request EffectRequest) error {
	if !destination.Valid() || !request.Mode.Valid() || request.EffectID == "" || request.Payload == "" ||
		request.Attempt < 1 {
		return fmt.Errorf("%w: destination, mode, effect ID, payload, and positive attempt are required", ErrInvalidEffect)
	}
	if !safePathComponent(request.EffectID) {
		return fmt.Errorf("%w: effect ID must be a safe path component", ErrInvalidEffect)
	}
	return nil
}

func safePathComponent(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
