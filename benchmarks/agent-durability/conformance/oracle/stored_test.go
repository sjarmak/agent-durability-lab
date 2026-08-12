package oracle

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestStoredVerdictDecoderRejectsAmbiguousAndTrailingJSON(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	tests := []struct {
		name string
		data string
	}{
		{name: "duplicate", data: `{"contract_version":"adl.cross-system.v1","contract_version":"adl.cross-system.v1"}`},
		{name: "trailing", data: `{}` + "\n{}"},
		{name: "unknown", data: `{"unknown":true}`},
	}
	for _, test := range tests {
		path := filepath.Join(root, test.name+".json")
		if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
			t.Fatalf("write %s: %v", test.name, err)
		}
		if _, err := readStoredVerdict(path); err == nil {
			t.Errorf("readStoredVerdict accepted %s input", test.name)
		}
	}
	missing := filepath.Join(root, strings.Repeat("x", 4))
	if _, err := readStoredVerdict(missing); !os.IsNotExist(err) {
		t.Fatalf("missing verdict error = %v", err)
	}
}

func TestExactEntriesRejectsFIFOAsAFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "artifact.json"), 0o600); err != nil {
		t.Fatalf("create FIFO: %v", err)
	}
	if problems := exactEntries(root, []string{"artifact.json"}, nil, false); len(problems) == 0 {
		t.Fatal("exactEntries accepted a FIFO as a regular artifact")
	}
}
