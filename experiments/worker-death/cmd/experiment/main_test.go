package main

import (
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/worker-death/internal/lab"
	"github.com/sjarmak/temporal_projects/internal/workstore"
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

func TestParsePostExecGapArms(t *testing.T) {
	all, err := parsePostExecGapArms("ALL")
	if err != nil {
		t.Fatalf("parse all: %v", err)
	}
	wantAll := []lab.PostExecGapArm{lab.PostExecAttachControl, lab.PostExecFencedReplacement}
	if !reflect.DeepEqual(all, wantAll) {
		t.Fatalf("all arms = %v; want %v", all, wantAll)
	}
	for _, arm := range wantAll {
		got, err := parsePostExecGapArms(string(arm))
		if err != nil || !reflect.DeepEqual(got, []lab.PostExecGapArm{arm}) {
			t.Errorf("parse %q = %v, %v", arm, got, err)
		}
	}
	if _, err := parsePostExecGapArms("invalid"); err == nil {
		t.Fatal("invalid arm returned nil error")
	}
}

func TestVariantRunID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, base, variant, want string
		variants, trial, trials   int
	}{
		{name: "single", base: "run", variant: "arm", variants: 1, trial: 1, trials: 1, want: "run"},
		{name: "multiple arms", base: "run", variant: "arm", variants: 2, trial: 1, trials: 1, want: "run-arm"},
		{name: "multiple trials", base: "run", variant: "arm", variants: 1, trial: 2, trials: 3, want: "run-trial-2"},
		{name: "matrix", base: "run", variant: "arm", variants: 2, trial: 3, trials: 3, want: "run-arm-trial-3"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := variantRunID(test.base, test.variant, test.variants, test.trial, test.trials); got != test.want {
				t.Fatalf("run ID = %q, want %q", got, test.want)
			}
		})
	}
}
