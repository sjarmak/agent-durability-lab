package lab

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendTrialProgressPreservesOrderedStages(t *testing.T) {
	directory := t.TempDir()
	if err := appendTrialProgress(directory, "await-entered", ""); err != nil {
		t.Fatalf("append first progress event: %v", err)
	}
	if err := appendTrialProgress(directory, "workflow-status", "running"); err != nil {
		t.Fatalf("append second progress event: %v", err)
	}
	file, err := os.Open(filepath.Join(directory, trialProgressFile))
	if err != nil {
		t.Fatalf("open progress journal: %v", err)
	}
	defer file.Close()
	var got []trialProgress
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event trialProgress
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode progress event: %v", err)
		}
		got = append(got, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan progress journal: %v", err)
	}
	if len(got) != 2 || got[0].Stage != "await-entered" ||
		got[1].Stage != "workflow-status" || got[1].Detail != "running" ||
		got[0].RecordedAt.IsZero() || got[1].RecordedAt.IsZero() {
		t.Fatalf("progress events = %+v", got)
	}
}

func TestAppendTrialProgressRejectsIncompleteIdentity(t *testing.T) {
	if err := appendTrialProgress("", "stage", ""); err == nil {
		t.Fatal("missing progress directory was accepted")
	}
	if err := appendTrialProgress(t.TempDir(), "", ""); err == nil {
		t.Fatal("missing progress stage was accepted")
	}
}
