package lab

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadStrictJSONRejectsNonRegularUnknownAndTrailingData(t *testing.T) {
	type record struct {
		Value string `json:"value"`
	}
	root := t.TempDir()
	valid := filepath.Join(root, "valid.json")
	if err := os.WriteFile(valid, []byte(`{"value":"ok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err := readStrictJSON[record](valid); err != nil || value.Value != "ok" {
		t.Fatalf("strict JSON = %+v, err=%v", value, err)
	}
	for name, data := range map[string]string{
		"unknown.json":   `{"value":"ok","extra":true}`,
		"trailing.json":  `{"value":"ok"} {}`,
		"malformed.json": `{`,
	} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readStrictJSON[record](path); err == nil {
			t.Fatalf("invalid strict JSON %s was accepted", name)
		}
	}
	if _, err := readStrictJSON[record](root); err == nil {
		t.Fatal("directory was accepted as evidence JSON")
	}
}
