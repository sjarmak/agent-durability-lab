package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/temporalio-labs/agent-durability-lab/internal/agentprocess"
	"github.com/temporalio-labs/agent-durability-lab/internal/agentsim"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func TestReadRequestAcceptsOneBoundedDocument(t *testing.T) {
	want := agentprocess.LaunchRequest{
		StorePath: "work.db", BarrierURL: "http://127.0.0.1",
		Config: agentsim.Config{Lease: workstore.Lease{SessionID: "session", Generation: 1, OwnerToken: "owner"}},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	got, err := readRequest(path)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if got.StorePath != want.StorePath || got.Config.Lease != want.Config.Lease {
		t.Fatalf("request = %+v; want %+v", got, want)
	}
}

func TestReadRequestRejectsMalformedUnknownAndTrailingData(t *testing.T) {
	for name, content := range map[string]string{
		"malformed": `not-json`,
		"unknown":   `{"store_path":"work.db","barrier_url":"http://127.0.0.1","config":{},"extra":true}`,
		"trailing":  `{} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatalf("write request: %v", err)
			}
			if _, err := readRequest(path); err == nil {
				t.Fatal("readRequest returned nil error")
			}
		})
	}
}
