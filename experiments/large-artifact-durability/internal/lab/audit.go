package lab

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	maxEvidenceJSONBytes = 16 << 20
	maxJSONDepth         = 64
	maxJSONItems         = 1 << 20
)

func AuditRun(root string) (Verdict, error) {
	bundle, manifest, err := loadAuditBundle(root)
	if err != nil {
		return Verdict{}, err
	}
	if manifest.SchemaVersion != 1 || manifest.Experiment != "large-artifact-durability" ||
		manifest.RunID != filepath.Base(root) || !manifest.Boundary.Valid() || !manifest.Mode.valid() {
		return Verdict{}, fmt.Errorf("%w: manifest identity is invalid", ErrInvalidArtifact)
	}
	if err := ValidateCurrentRuntimeProvenance(manifest.Runtime); err != nil {
		return Verdict{}, err
	}
	if err := ValidateCurrentSourcePins(manifest.SourcePins); err != nil {
		return Verdict{}, err
	}
	var evidence Evidence
	if err := bundle.decode("evidence.json", &evidence); err != nil {
		return Verdict{}, err
	}
	var storedVerdict Verdict
	if err := bundle.decode("verdict.json", &storedVerdict); err != nil {
		return Verdict{}, err
	}
	historyData := bundle.files["temporal-history.json"]
	if err := rejectDuplicateJSONKeys(historyData); err != nil {
		return Verdict{}, err
	}
	history := &historypb.History{}
	if err := protojson.Unmarshal(historyData, history); err != nil {
		return Verdict{}, fmt.Errorf("decode Temporal history: %w", err)
	}
	if evidence.Boundary != manifest.Boundary || evidence.Mode != manifest.Mode ||
		evidence.Barrier.SessionID != manifest.ArtifactID {
		return Verdict{}, fmt.Errorf("%w: evidence identity differs from manifest", ErrInvalidArtifact)
	}
	if manifest.Runtime.CapturedAt.After(manifest.StartedAt) || manifest.StartedAt.After(evidence.Barrier.Time) ||
		evidence.Barrier.Time.After(evidence.Kill.KilledAt) || evidence.Kill.KilledAt.After(manifest.CompletedAt) {
		return Verdict{}, fmt.Errorf("%w: evidence timestamps are not ordered", ErrInvalidArtifact)
	}
	source := bundle.files["source-artifact.bin"]
	if digestBytes(source) != manifest.ArtifactSHA256 || int64(len(source)) != manifest.ArtifactSize {
		return Verdict{}, fmt.Errorf("%w: source artifact differs from manifest", ErrArtifactConflict)
	}
	reconstructedHistory := summarizeHistory(history)
	if !reflect.DeepEqual(reconstructedHistory, evidence.History) {
		return Verdict{}, fmt.Errorf("%w: stored history observation %+v differs from raw history %+v",
			ErrArtifactConflict, evidence.History, reconstructedHistory)
	}
	if manifest.Boundary == BoundaryExternalStorageStored {
		entries := bundle.entries(filepath.Join("external-store", externalObjectsDirectory))
		if !reflect.DeepEqual(entries, evidence.FinalExternalStore.Blobs) {
			return Verdict{}, fmt.Errorf("%w: external object inventory differs", ErrArtifactConflict)
		}
		if evidence.ExternalResult.Digest != manifest.ArtifactSHA256 || evidence.ExternalResult.Size != manifest.ArtifactSize {
			return Verdict{}, fmt.Errorf("%w: external Workflow result differs from source", ErrArtifactConflict)
		}
	} else {
		final := bundle.snapshot("artifact-store")
		if !reflect.DeepEqual(final, evidence.FinalStore) {
			return Verdict{}, fmt.Errorf("%w: application store inventory %+v differs from recorded %+v",
				ErrArtifactConflict, final, evidence.FinalStore)
		}
		artifact, err := validateBundleArtifact(bundle, evidence.WorkflowResult)
		if err != nil {
			return Verdict{}, err
		}
		if digestBytes(artifact) != manifest.ArtifactSHA256 || int64(len(artifact)) != manifest.ArtifactSize ||
			evidence.WorkflowResult.Acknowledgement.Digest != manifest.ArtifactSHA256 {
			return Verdict{}, fmt.Errorf("%w: Workflow reference/acknowledgement differs from source", ErrArtifactConflict)
		}
	}
	storage := converter.ExternalStorage{}
	if manifest.Boundary == BoundaryExternalStorageStored {
		storage = auditReplayStorage(manifest.Mode, bundle.files)
	}
	if err := replayHistoryWithExternalStorage(history, storage); err != nil {
		return Verdict{}, err
	}
	computed := Verify(evidence)
	if !reflect.DeepEqual(computed, storedVerdict) {
		return Verdict{}, fmt.Errorf("%w: stored verdict differs from recomputation", ErrArtifactConflict)
	}
	return computed, nil
}

func validateBundleArtifact(bundle auditBundle, result WorkflowResult) ([]byte, error) {
	reference := result.Reference
	var durable ArtifactReference
	referencePath := filepath.ToSlash(filepath.Join("artifact-store", referencesDirectory, reference.ReferenceName))
	if err := bundle.decode(referencePath, &durable); err != nil {
		return nil, err
	}
	if durable != reference {
		return nil, fmt.Errorf("%w: Workflow reference differs from durable reference", ErrArtifactConflict)
	}
	blobPath := filepath.ToSlash(filepath.Join("artifact-store", blobsDirectory, reference.BlobName))
	artifact, found := bundle.files[blobPath]
	if !found || int64(len(artifact)) != reference.Size || digestBytes(artifact) != reference.Digest {
		return nil, fmt.Errorf("%w: artifact blob differs from durable reference", ErrArtifactConflict)
	}
	acknowledgementFound := false
	ackPrefix := filepath.ToSlash(filepath.Join("artifact-store", acknowledgementsDirectory)) + "/"
	for name := range bundle.files {
		if !strings.HasPrefix(name, ackPrefix) || strings.Contains(strings.TrimPrefix(name, ackPrefix), "/") {
			continue
		}
		var acknowledgement Acknowledgement
		if err := bundle.decode(name, &acknowledgement); err != nil {
			return nil, err
		}
		acknowledgementFound = acknowledgementFound || acknowledgement == result.Acknowledgement
	}
	if !acknowledgementFound {
		return nil, fmt.Errorf("%w: Workflow acknowledgement is not durable", ErrArtifactConflict)
	}
	return artifact, nil
}

func validateManifestInventory(root string, files map[string]string) error {
	return validateManifestInventoryWithDirectories(root, files, nil)
}

func validateManifestInventoryWithDirectories(root string, files map[string]string, expectedDirectories []string) error {
	actualFiles, actualDirectories, err := evidenceInventory(root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actualFiles, files) {
		return fmt.Errorf("%w: evidence file inventory or digest differs", ErrArtifactConflict)
	}
	if expectedDirectories != nil {
		sort.Strings(expectedDirectories)
		sort.Strings(actualDirectories)
		if !reflect.DeepEqual(actualDirectories, expectedDirectories) {
			return fmt.Errorf("%w: evidence directory inventory differs", ErrArtifactConflict)
		}
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON document has trailing data")
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	if len(data) > maxEvidenceJSONBytes || !utf8.Valid(data) {
		return errors.New("JSON document is not bounded UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder, 1); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON document has trailing data")
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("unexpected JSON delimiter")
	}
	seen := make(map[string]struct{})
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONItems {
			return errors.New("JSON collection exceeds limit")
		}
		if delimiter == '{' {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, found := seen[key]; found {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
		}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}
