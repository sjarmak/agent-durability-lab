package evidence

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

// RunIdentity names one immutable benchmark trial.
type RunIdentity struct {
	RunID     string
	Case      protocol.CaseID
	Probe     protocol.Probe
	Trial     int
	SessionID string
}

// Bundle contains adapter-captured raw evidence. It cannot contain a verdict.
type Bundle struct {
	Identity    RunIdentity
	Events      []protocol.Event
	Authority   protocol.AuthorityState
	Destination protocol.DestinationState
	Fault       protocol.FaultBoundary
	Processes   []protocol.ProcessObservation
	Native      []protocol.NativeRecord
	Input       protocol.EffectiveInput
}

// WriteRun validates and exclusively publishes one raw evidence bundle.
func WriteRun(ctx context.Context, root string, bundle Bundle) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateBundle(root, bundle); err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", fmt.Errorf("create evidence root: %w", err)
	}
	runDir := filepath.Join(root, bundle.Identity.RunID)
	if err := os.Mkdir(runDir, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return runDir, fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, runDir)
		}
		return runDir, fmt.Errorf("create run evidence: %w", err)
	}
	if err := writeBundle(ctx, runDir, bundle); err != nil {
		return runDir, err
	}
	if err := syncDirectory(runDir); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func validateBundle(root string, bundle Bundle) error {
	if root == "" || bundle.Identity.RunID == "" || bundle.Identity.RunID == "." || bundle.Identity.RunID == ".." ||
		bundle.Identity.RunID != filepath.Base(bundle.Identity.RunID) ||
		!bundle.Identity.Case.Valid() || !bundle.Identity.Probe.Valid() || bundle.Identity.Trial < 1 || bundle.Identity.SessionID == "" {
		return fmt.Errorf("%w: complete run identity and evidence root are required", protocol.ErrInvalidEvidence)
	}
	if err := bundle.Input.Validate(); err != nil {
		return err
	}
	if err := bundle.Authority.Validate(bundle.Identity.SessionID); err != nil {
		return err
	}
	if err := bundle.Destination.Validate(); err != nil {
		return err
	}
	if bundle.Input.DestinationID != bundle.Destination.DestinationID {
		return fmt.Errorf("%w: effective input and destination identity differ", protocol.ErrInvalidEvidence)
	}
	if err := validateEvents(bundle.Identity.SessionID, bundle.Events); err != nil {
		return err
	}
	if err := validateProcesses(bundle.Processes, uint64(len(bundle.Events))); err != nil {
		return err
	}
	if err := validateNative(bundle.Native); err != nil {
		return err
	}
	if err := validateFault(bundle.Identity.Probe, bundle.Fault, bundle.Events); err != nil {
		return err
	}
	return nil
}

func validateEvents(sessionID string, events []protocol.Event) error {
	if len(events) == 0 {
		return fmt.Errorf("%w: common event stream is empty", protocol.ErrInvalidEvidence)
	}
	var previousTime time.Time
	for index, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		if event.SessionID != sessionID || event.Sequence != uint64(index+1) {
			return fmt.Errorf("%w: common event identity or sequence is inconsistent", protocol.ErrInvalidEvidence)
		}
		eventTime, _ := time.Parse(time.RFC3339Nano, event.Time)
		if !previousTime.IsZero() && !eventTime.After(previousTime) {
			return fmt.Errorf("%w: common event time is not strictly increasing", protocol.ErrInvalidEvidence)
		}
		previousTime = eventTime
	}
	return nil
}

func validateProcesses(processes []protocol.ProcessObservation, eventCount uint64) error {
	if len(processes) == 0 {
		return fmt.Errorf("%w: process observations are empty", protocol.ErrInvalidEvidence)
	}
	var previous uint64
	for _, process := range processes {
		if err := process.Validate(); err != nil {
			return err
		}
		if process.Sequence <= previous || process.Sequence > eventCount {
			return fmt.Errorf("%w: process observation sequence is inconsistent", protocol.ErrInvalidEvidence)
		}
		previous = process.Sequence
	}
	return nil
}

func validateNative(records []protocol.NativeRecord) error {
	if len(records) == 0 {
		return fmt.Errorf("%w: native journal is empty", protocol.ErrInvalidEvidence)
	}
	for index, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if record.Sequence != uint64(index+1) {
			return fmt.Errorf("%w: native journal sequence is inconsistent", protocol.ErrInvalidEvidence)
		}
	}
	return nil
}

func validateFault(probe protocol.Probe, fault protocol.FaultBoundary, events []protocol.Event) error {
	if probe == protocol.ProbeUnfaulted {
		if fault.Triggered || fault.Point != "" || fault.AfterSequence != 0 || fault.BeforeSequence != 0 ||
			fault.ActorID != "" || fault.ProcessIdentity != "" || fault.TriggeredAt != "" {
			return fmt.Errorf("%w: unfaulted run contains a fault", protocol.ErrInvalidEvidence)
		}
		return nil
	}
	if !fault.Triggered || fault.Point == "" || fault.AfterSequence == 0 || fault.BeforeSequence <= fault.AfterSequence ||
		fault.ActorID == "" || fault.ProcessIdentity == "" || fault.TriggeredAt == "" {
		return fmt.Errorf("%w: faulted run lacks an exact boundary", protocol.ErrInvalidEvidence)
	}
	if fault.BeforeSequence > uint64(len(events)) {
		return fmt.Errorf("%w: fault boundary references an absent event", protocol.ErrInvalidEvidence)
	}
	faultTime, err := time.Parse(time.RFC3339Nano, fault.TriggeredAt)
	if err != nil {
		return fmt.Errorf("%w: fault time: %v", protocol.ErrInvalidEvidence, err)
	}
	afterTime, _ := time.Parse(time.RFC3339Nano, events[fault.AfterSequence-1].Time)
	beforeTime, _ := time.Parse(time.RFC3339Nano, events[fault.BeforeSequence-1].Time)
	if !faultTime.After(afterTime) || !faultTime.Before(beforeTime) {
		return fmt.Errorf("%w: fault time is outside the declared boundary", protocol.ErrInvalidEvidence)
	}
	return nil
}

func writeBundle(ctx context.Context, runDir string, bundle Bundle) error {
	files := map[string]any{
		protocol.AuthorityStateFile:      bundle.Authority,
		protocol.DestinationStateFile:    bundle.Destination,
		protocol.FaultBoundaryFile:       bundle.Fault,
		protocol.NativeJournalFile:       bundle.Native,
		protocol.ProcessObservationsFile: bundle.Processes,
		protocol.EffectiveInputFile:      bundle.Input,
	}
	names := make([]string, 0, len(files)+1)
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeJSONExclusive(ctx, filepath.Join(runDir, name), files[name]); err != nil {
			return err
		}
	}
	if err := writeJSONLExclusive(ctx, filepath.Join(runDir, protocol.CommonEventsFile), bundle.Events); err != nil {
		return err
	}
	names = append(names, protocol.CommonEventsFile)
	sort.Strings(names)

	manifest := protocol.Manifest{
		ContractVersion: protocol.ContractVersion,
		RunID:           bundle.Identity.RunID,
		Case:            bundle.Identity.Case,
		Probe:           bundle.Identity.Probe,
		Trial:           bundle.Identity.Trial,
		SessionID:       bundle.Identity.SessionID,
		EvidenceSHA256:  make(map[string]string, len(names)),
	}
	for _, name := range names {
		hash, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			return fmt.Errorf("hash %s: %w", name, err)
		}
		manifest.EvidenceSHA256[name] = hash
	}
	manifest.InputSHA256 = manifest.EvidenceSHA256[protocol.EffectiveInputFile]
	if err := manifest.Validate(); err != nil {
		return err
	}
	return writeJSONExclusive(ctx, filepath.Join(runDir, protocol.ManifestFile), manifest)
}

func writeJSONExclusive(ctx context.Context, path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return writeExclusive(ctx, path, append(data, '\n'))
}

func writeJSONLExclusive(ctx context.Context, path string, events []protocol.Event) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return err
		}
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			return fmt.Errorf("encode event: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("flush events: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync events: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close events: %w", err)
	}
	return nil
}

func writeExclusive(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return evidenceWriteError(path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func evidenceWriteError(path string, err error) error {
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: %s", protocol.ErrEvidenceExists, path)
	}
	return fmt.Errorf("create %s: %w", filepath.Base(path), err)
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
