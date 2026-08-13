package presentation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	maxJSONDepth           = 64
	maxJSONCollectionItems = 10_000
)

func DecodeJSON(data []byte) (Catalog, error) {
	if err := inspectJSON(data); err != nil {
		return Catalog{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var catalog Catalog
	if err := decoder.Decode(&catalog); err != nil {
		return Catalog{}, fmt.Errorf("decode presentation catalog: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Catalog{}, err
	}
	if err := Validate(catalog); err != nil {
		return Catalog{}, fmt.Errorf("validate presentation catalog: %w", err)
	}
	return catalog, nil
}

func inspectJSON(data []byte) error {
	if len(data) > MaxJSONDocumentBytes {
		return fmt.Errorf("JSON document exceeds %d bytes", MaxJSONDocumentBytes)
	}
	if !utf8.Valid(data) {
		return errors.New("JSON document is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder, 1); err != nil {
		return fmt.Errorf("inspect presentation JSON: %w", err)
	}
	return requireJSONEOF(decoder)
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds %d levels", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		return scanJSONObject(decoder, depth)
	case '[':
		return scanJSONArray(decoder, depth)
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

func scanJSONObject(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{})
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONCollectionItems {
			return fmt.Errorf("JSON object exceeds %d fields", maxJSONCollectionItems)
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("object key is not a string")
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate object key %q", key)
		}
		seen[key] = struct{}{}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func scanJSONArray(decoder *json.Decoder, depth int) error {
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONCollectionItems {
			return fmt.Errorf("JSON array exceeds %d items", maxJSONCollectionItems)
		}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return fmt.Errorf("read trailing JSON: %w", err)
	}
	return nil
}
