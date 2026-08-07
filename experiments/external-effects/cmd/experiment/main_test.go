package main

import (
	"testing"

	"github.com/temporalio-labs/agent-durability-lab/experiments/external-effects/internal/lab"
)

func TestParseDestinationsAndModes(t *testing.T) {
	t.Parallel()
	destinations, err := parseDestinations("ALL")
	if err != nil || len(destinations) != len(lab.AllDestinations()) {
		t.Fatalf("parse all destinations = %v, %v", destinations, err)
	}
	modes, err := parseModes("all")
	if err != nil || len(modes) != 2 {
		t.Fatalf("parse all modes = %v, %v", modes, err)
	}
	if _, err := parseDestinations("unknown"); err == nil {
		t.Fatal("unknown destination succeeded")
	}
	if _, err := parseModes("unknown"); err == nil {
		t.Fatal("unknown mode succeeded")
	}
}
