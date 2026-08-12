package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const maxEvidenceJSONBytes = 64 << 20

func readStrictJSON[T any](path string) (value T, returnErr error) {
	info, err := os.Lstat(path)
	if err != nil {
		return value, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxEvidenceJSONBytes {
		return value, fmt.Errorf("evidence JSON %q is not a bounded regular file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return value, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	decoder := json.NewDecoder(io.LimitReader(file, maxEvidenceJSONBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode evidence JSON %q: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("evidence JSON %q contains trailing data", path)
	}
	return value, nil
}
