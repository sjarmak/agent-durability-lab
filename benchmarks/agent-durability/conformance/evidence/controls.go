package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	legacyprotocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

type InvalidControlSpec struct {
	ID             string
	SourceCase     legacyprotocol.CaseID
	ExpectedReason string
}

var invalidControlSpecs = []InvalidControlSpec{
	{ID: "malformed", SourceCase: legacyprotocol.CaseAmbiguousEffect, ExpectedReason: legacyprotocol.ReasonEvidenceMalformed},
	{ID: "missed-boundary", SourceCase: legacyprotocol.CaseSurvivingExecutor, ExpectedReason: legacyprotocol.ReasonFaultNotBracketed},
	{ID: "wrong-process-identity", SourceCase: legacyprotocol.CaseAmbiguousEffect, ExpectedReason: legacyprotocol.ReasonWrongProcessIdentity},
	{ID: "contradiction", SourceCase: legacyprotocol.CaseStaleGeneration, ExpectedReason: legacyprotocol.ReasonEvidenceInconsistent},
}

func InvalidControlSpecs() []InvalidControlSpec {
	return append([]InvalidControlSpec(nil), invalidControlSpecs...)
}

func WriteInvalidControl(ctx context.Context, root string, spec InvalidControlSpec, sourceRunDir string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !knownControl(spec) || root == "" || sourceRunDir == "" {
		return "", fmt.Errorf("%w: unsupported invalid control", legacyprotocol.ErrInvalidEvidence)
	}
	files := make(map[string][]byte, len(legacyprotocol.RawEvidenceFiles()))
	for _, name := range legacyprotocol.RawEvidenceFiles() {
		data, err := os.ReadFile(filepath.Join(sourceRunDir, name))
		if err != nil {
			return "", fmt.Errorf("read source %s: %w", name, err)
		}
		files[name] = data
	}
	var manifest legacyprotocol.Manifest
	if err := DecodeJSONStrict(files[legacyprotocol.ManifestFile], &manifest); err != nil {
		return "", fmt.Errorf("decode source manifest: %w", err)
	}
	if manifest.Case != spec.SourceCase || manifest.Probe != legacyprotocol.ProbeProtected || manifest.Trial != 1 {
		return "", fmt.Errorf("%w: invalid control source identity differs", legacyprotocol.ErrInvalidEvidence)
	}
	manifest.RunID = "invalid-control-" + spec.ID
	changedName, changedData, err := mutateControl(spec.ID, files)
	if err != nil {
		return "", err
	}
	files[changedName] = changedData
	manifest.EvidenceSHA256[changedName] = sha256Hex(changedData)
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode control manifest: %w", err)
	}
	files[legacyprotocol.ManifestFile] = append(manifestData, '\n')

	controlDir := filepath.Join(root, spec.ID)
	if err := os.Mkdir(controlDir, 0o750); err != nil {
		if errors.Is(err, os.ErrExist) {
			return controlDir, fmt.Errorf("%w: %s", legacyprotocol.ErrEvidenceExists, controlDir)
		}
		return controlDir, fmt.Errorf("create invalid control: %w", err)
	}
	for _, name := range legacyprotocol.RawEvidenceFiles()[1:] {
		if err := writeExclusiveFile(ctx, filepath.Join(controlDir, name), files[name]); err != nil {
			return controlDir, err
		}
	}
	if err := writeExclusiveFile(ctx, filepath.Join(controlDir, legacyprotocol.ManifestFile), files[legacyprotocol.ManifestFile]); err != nil {
		return controlDir, err
	}
	if err := syncDirectory(controlDir); err != nil {
		return controlDir, err
	}
	return controlDir, nil
}

func knownControl(spec InvalidControlSpec) bool {
	for _, known := range invalidControlSpecs {
		if spec == known {
			return true
		}
	}
	return false
}

func mutateControl(id string, files map[string][]byte) (string, []byte, error) {
	switch id {
	case "malformed":
		data, err := duplicateFirstObjectKey(files[legacyprotocol.AuthorityStateFile])
		return legacyprotocol.AuthorityStateFile, data, err
	case "missed-boundary":
		var fault legacyprotocol.FaultBoundary
		if err := json.Unmarshal(files[legacyprotocol.FaultBoundaryFile], &fault); err != nil {
			return "", nil, err
		}
		fault.AfterSequence = fault.BeforeSequence
		data, err := marshalLine(fault)
		return legacyprotocol.FaultBoundaryFile, data, err
	case "wrong-process-identity":
		var fault legacyprotocol.FaultBoundary
		if err := json.Unmarshal(files[legacyprotocol.FaultBoundaryFile], &fault); err != nil {
			return "", nil, err
		}
		fault.ProcessIdentity = "pid:999:start:wrong"
		data, err := marshalLine(fault)
		return legacyprotocol.FaultBoundaryFile, data, err
	case "contradiction":
		var authority legacyprotocol.AuthorityState
		if err := json.Unmarshal(files[legacyprotocol.AuthorityStateFile], &authority); err != nil {
			return "", nil, err
		}
		authority.ActiveGeneration = 1
		data, err := marshalLine(authority)
		return legacyprotocol.AuthorityStateFile, data, err
	default:
		return "", nil, fmt.Errorf("%w: unsupported invalid control %q", legacyprotocol.ErrInvalidEvidence, id)
	}
}

func duplicateFirstObjectKey(data []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("decode malformed-control source: %w", err)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, errors.New("malformed-control source is empty")
	}
	sort.Strings(keys)
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, errors.New("malformed-control source is not an object")
	}
	keyName := keys[0]
	key, err := json.Marshal(keyName)
	if err != nil {
		return nil, err
	}
	result := make([]byte, 0, len(data)+len(key)+len(fields[keyName])+8)
	result = append(result, '{', '\n', ' ', ' ')
	result = append(result, key...)
	result = append(result, ':', ' ')
	result = append(result, fields[keyName]...)
	result = append(result, ',')
	result = append(result, trimmed[1:]...)
	result = append(result, '\n')
	return result, nil
}

func marshalLine(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
