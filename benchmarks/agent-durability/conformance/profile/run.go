package profile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/evidence"
	conformanceoracle "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/conformance/oracle"
	legacyoracle "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type Config struct {
	EvidenceRoot   string
	SourceRoot     string
	SchemaRoot     string
	ExecutablePath string
}

func RunCalibration(ctx context.Context, config Config) (evidence.Report, error) {
	if err := validateConfig(config); err != nil {
		return evidence.Report{}, err
	}
	if _, err := os.Lstat(config.EvidenceRoot); err == nil {
		return evidence.Report{}, fmt.Errorf("%w: %s", legacyprotocol.ErrEvidenceExists, config.EvidenceRoot)
	} else if !errors.Is(err, os.ErrNotExist) {
		return evidence.Report{}, fmt.Errorf("inspect evidence root: %w", err)
	}
	pins, err := evidence.CapturePins(config.ExecutablePath, config.SourceRoot, config.SchemaRoot)
	if err != nil {
		return evidence.Report{}, err
	}
	if err := os.MkdirAll(filepath.Dir(config.EvidenceRoot), 0o750); err != nil {
		return evidence.Report{}, fmt.Errorf("create evidence parent: %w", err)
	}
	if err := os.Mkdir(config.EvidenceRoot, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return evidence.Report{}, fmt.Errorf("%w: %s", legacyprotocol.ErrEvidenceExists, config.EvidenceRoot)
		}
		return evidence.Report{}, fmt.Errorf("create evidence root: %w", err)
	}
	if _, err := evidence.PreserveExecutable(ctx, config.EvidenceRoot, config.ExecutablePath, pins.Executable); err != nil {
		return evidence.Report{}, fmt.Errorf("preserve conformance executable: %w", err)
	}
	runsRoot := filepath.Join(config.EvidenceRoot, "runs")
	invalidRoot := filepath.Join(config.EvidenceRoot, "invalid-controls")
	for _, path := range []string{runsRoot, invalidRoot} {
		if err := os.Mkdir(path, 0o750); err != nil {
			return evidence.Report{}, fmt.Errorf("create suite directory %s: %w", filepath.Base(path), err)
		}
	}
	for _, spec := range Plan() {
		if err := ctx.Err(); err != nil {
			return evidence.Report{}, err
		}
		runDir, err := calibration.Run(ctx, calibration.Config{Root: runsRoot, Case: spec.Case, Probe: spec.Probe, Trial: spec.Trial})
		if err != nil {
			return evidence.Report{}, fmt.Errorf("run %s/%s trial %d: %w", spec.Case, spec.Probe, spec.Trial, err)
		}
		if _, err := legacyoracle.EvaluateAndWrite(ctx, runDir); err != nil {
			return evidence.Report{}, fmt.Errorf("evaluate %s/%s trial %d: %w", spec.Case, spec.Probe, spec.Trial, err)
		}
	}
	for _, spec := range evidence.InvalidControlSpecs() {
		sourceRunID := fmt.Sprintf("%s-%s-trial-1", spec.SourceCase, legacyprotocol.ProbeProtected)
		controlDir, err := evidence.WriteInvalidControl(ctx, invalidRoot, spec, filepath.Join(runsRoot, sourceRunID))
		if err != nil {
			return evidence.Report{}, fmt.Errorf("write invalid control %s: %w", spec.ID, err)
		}
		if _, err := legacyoracle.EvaluateAndWrite(ctx, controlDir); err != nil {
			return evidence.Report{}, fmt.Errorf("evaluate invalid control %s: %w", spec.ID, err)
		}
	}
	report, evaluationErr := conformanceoracle.Evaluate(ctx, config.EvidenceRoot, pins)
	if _, err := evidence.WriteReport(ctx, config.EvidenceRoot, report); err != nil {
		return report, errors.Join(evaluationErr, err)
	}
	return report, evaluationErr
}

func validateConfig(config Config) error {
	if config.EvidenceRoot == "" || filepath.Clean(config.EvidenceRoot) == "." || config.SourceRoot == "" || config.SchemaRoot == "" || config.ExecutablePath == "" {
		return fmt.Errorf("%w: evidence root, source root, schema root, and executable are required", legacyprotocol.ErrInvalidEvidence)
	}
	if err := ctxFreePath(config.EvidenceRoot); err != nil {
		return err
	}
	return nil
}

func ctxFreePath(path string) error {
	clean := filepath.Clean(path)
	if clean == string(filepath.Separator) {
		return fmt.Errorf("%w: evidence root is too broad", legacyprotocol.ErrInvalidEvidence)
	}
	return nil
}
