package lab

import (
	"fmt"
	"path/filepath"
	"reflect"
	"time"
)

const (
	runtimePreregistrationSchema = "large-artifact-runtime-preregistration-v1"
	runtimePreregistrationPath   = "experiments/large-artifact-durability/runtime-preregistration-v1.json"
)

type RuntimePreregistration struct {
	Schema               string `json:"schema"`
	OS                   string `json:"os"`
	Architecture         string `json:"architecture"`
	GoVersion            string `json:"go_version"`
	SDKVersion           string `json:"sdk_version"`
	TemporalVersion      string `json:"temporal_version"`
	TemporalSHA256       string `json:"temporal_sha256"`
	TemporalBytes        int64  `json:"temporal_bytes"`
	WorkerSHA256         string `json:"worker_sha256"`
	WorkerBytes          int64  `json:"worker_bytes"`
	CoverageWorkerSHA256 string `json:"coverage_worker_sha256"`
	CoverageWorkerBytes  int64  `json:"coverage_worker_bytes"`
}

func LoadRuntimePreregistration() (RuntimePreregistration, error) {
	repositoryRoot, err := sourceRepositoryRoot()
	if err != nil {
		return RuntimePreregistration{}, err
	}
	data, err := readBoundedRegular(filepath.Join(repositoryRoot, filepath.FromSlash(runtimePreregistrationPath)), maxRecordBytes)
	if err != nil {
		return RuntimePreregistration{}, fmt.Errorf("read runtime preregistration: %w", err)
	}
	var registration RuntimePreregistration
	if err := decodeStrictJSON(data, &registration); err != nil {
		return RuntimePreregistration{}, fmt.Errorf("decode runtime preregistration: %w", err)
	}
	if registration.Schema != runtimePreregistrationSchema {
		return RuntimePreregistration{}, fmt.Errorf("%w: runtime preregistration schema is invalid", ErrInvalidArtifact)
	}
	if err := validateRuntimeProvenance(registration.Provenance(time.Unix(0, 0).UTC())); err != nil {
		return RuntimePreregistration{}, fmt.Errorf("validate runtime preregistration: %w", err)
	}
	coverage := registration.Provenance(time.Unix(0, 0).UTC())
	coverage.WorkerSHA256 = registration.CoverageWorkerSHA256
	coverage.WorkerBytes = registration.CoverageWorkerBytes
	if err := validateRuntimeProvenance(coverage); err != nil {
		return RuntimePreregistration{}, fmt.Errorf("validate coverage runtime preregistration: %w", err)
	}
	return registration, nil
}

func (r RuntimePreregistration) Provenance(capturedAt time.Time) RuntimeProvenance {
	return RuntimeProvenance{
		CapturedAt: capturedAt, OS: r.OS, Architecture: r.Architecture,
		GoVersion: r.GoVersion, SDKVersion: r.SDKVersion, TemporalVersion: r.TemporalVersion,
		TemporalSHA256: r.TemporalSHA256, TemporalBytes: r.TemporalBytes,
		WorkerSHA256: r.WorkerSHA256, WorkerBytes: r.WorkerBytes,
	}
}

func ValidateCurrentRuntimeProvenance(value RuntimeProvenance) error {
	if err := validateRuntimeProvenance(value); err != nil {
		return err
	}
	registered, err := LoadRuntimePreregistration()
	if err != nil {
		return err
	}
	canonical := registered.Provenance(value.CapturedAt)
	coverage := canonical
	coverage.WorkerSHA256 = registered.CoverageWorkerSHA256
	coverage.WorkerBytes = registered.CoverageWorkerBytes
	if !reflect.DeepEqual(canonical, value) && !reflect.DeepEqual(coverage, value) {
		return fmt.Errorf("%w: runtime provenance differs from current preregistration", ErrArtifactConflict)
	}
	return nil
}
