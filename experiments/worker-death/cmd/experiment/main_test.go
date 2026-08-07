package main

import (
	"reflect"
	"testing"

	"github.com/temporalio-labs/agent-durability-lab/experiments/worker-death/internal/lab"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
)

func TestParseModes(t *testing.T) {
	all, err := parseModes("ALL")
	if err != nil {
		t.Fatalf("parse all: %v", err)
	}
	wantAll := []workstore.Mode{workstore.ModeUnsafe, workstore.ModeReattach, workstore.ModeFenced}
	if !reflect.DeepEqual(all, wantAll) {
		t.Fatalf("all modes = %v; want %v", all, wantAll)
	}
	for _, mode := range wantAll {
		got, err := parseModes(string(mode))
		if err != nil || !reflect.DeepEqual(got, []workstore.Mode{mode}) {
			t.Errorf("parse %q = %v, %v", mode, got, err)
		}
	}
	if _, err := parseModes("invalid"); err == nil {
		t.Fatal("invalid mode returned nil error")
	}
}

func TestParseLaunchGapArms(t *testing.T) {
	all, err := parseLaunchGapArms("ALL")
	if err != nil {
		t.Fatalf("parse all: %v", err)
	}
	wantAll := []lab.LaunchGapArm{lab.LaunchGapControl, lab.LaunchGapFencedRecovery}
	if !reflect.DeepEqual(all, wantAll) {
		t.Fatalf("all arms = %v; want %v", all, wantAll)
	}
	for _, arm := range wantAll {
		got, err := parseLaunchGapArms(string(arm))
		if err != nil || !reflect.DeepEqual(got, []lab.LaunchGapArm{arm}) {
			t.Errorf("parse %q = %v, %v", arm, got, err)
		}
	}
	if _, err := parseLaunchGapArms("invalid"); err == nil {
		t.Fatal("invalid arm returned nil error")
	}
}
