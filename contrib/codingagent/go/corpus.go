package codingagent

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var schemaFiles = map[string]string{
	"identity": "identity.schema.json", "transition": "transition.schema.json",
	"event": "event.schema.json", "evidence": "evidence.schema.json",
}

// SchemaCorpus adapts the authoritative language-neutral schemas at an
// external JSON boundary. Loading schemas performs file IO and must not occur
// inside Workflow code.
type SchemaCorpus struct{ schemas map[string]*jsonschema.Schema }

func LoadSchemaCorpus(schemaDirectory string) (SchemaCorpus, error) {
	if schemaDirectory == "" || !filepath.IsAbs(schemaDirectory) {
		return SchemaCorpus{}, fmt.Errorf("%w: schema directory must be absolute", ErrInvalidInput)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schemas := make(map[string]*jsonschema.Schema, len(schemaFiles))
	for kind, name := range schemaFiles {
		schema, err := compiler.Compile(filepath.Join(schemaDirectory, name))
		if err != nil {
			return SchemaCorpus{}, fmt.Errorf("compile %s schema: %w", kind, err)
		}
		schemas[kind] = schema
	}
	return SchemaCorpus{schemas: schemas}, nil
}

func (corpus SchemaCorpus) Validate(kind string, data []byte) error {
	schema, ok := corpus.schemas[kind]
	if !ok {
		return fmt.Errorf("%w: unknown schema kind %q", ErrInvalidInput, kind)
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return fmt.Errorf("validate %s JSON: %w", kind, err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("decode %s JSON: %w", kind, err)
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("validate %s schema: %w", kind, err)
	}
	if kind == "evidence" {
		if err := validateEvidencePaths(instance); err != nil {
			return fmt.Errorf("validate evidence paths: %w", err)
		}
	}
	return nil
}

func validateEvidencePaths(instance any) error {
	document, ok := instance.(map[string]any)
	if !ok {
		return fmt.Errorf("evidence must be an object")
	}
	artifacts, _ := document["artifacts"].([]any)
	for _, value := range artifacts {
		artifact, _ := value.(map[string]any)
		artifactPath, _ := artifact["path"].(string)
		if !confinedArtifactPath(artifactPath) {
			return fmt.Errorf("artifact path %q is not confined", artifactPath)
		}
	}
	replay, _ := document["replay"].(map[string]any)
	historyPath, _ := replay["history_path"].(string)
	if !confinedArtifactPath(historyPath) {
		return fmt.Errorf("history path %q is not confined", historyPath)
	}
	observations, _ := document["observations"].([]any)
	for _, value := range observations {
		observation, _ := value.(map[string]any)
		reference, _ := observation["reference"].(map[string]any)
		if artifactPath, present := reference["artifact_path"].(string); present && !confinedArtifactPath(artifactPath) {
			return fmt.Errorf("observation artifact path %q is not confined", artifactPath)
		}
	}
	return nil
}
