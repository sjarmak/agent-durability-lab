package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/large-artifact-durability/internal/lab"
)

func TestRunAuditsOneExpectedRun(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	called := ""
	err := run([]string{"/evidence/run-1"}, &output, func(root string) (lab.Verdict, error) {
		called = root
		return lab.Verdict{RunValid: true, ExpectedObservation: true, InvariantSatisfied: true}, nil
	}, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if called != "/evidence/run-1" || !strings.Contains(output.String(), `"run_valid": true`) {
		t.Fatalf("called = %q, output = %q", called, output.String())
	}
}

func TestRunFailsClosed(t *testing.T) {
	t.Parallel()

	if err := run(nil, &bytes.Buffer{}, nil, nil); err == nil {
		t.Fatal("missing run directory accepted")
	}
	want := errors.New("audit failed")
	if err := run([]string{"run"}, &bytes.Buffer{}, func(string) (lab.Verdict, error) {
		return lab.Verdict{}, want
	}, nil); !errors.Is(err, want) {
		t.Fatalf("audit error = %v", err)
	}
	if err := run([]string{"run"}, &bytes.Buffer{}, func(string) (lab.Verdict, error) {
		return lab.Verdict{RunValid: true}, nil
	}, nil); err == nil {
		t.Fatal("unexpected verdict accepted")
	}
}

func TestRunAuditsPopulation(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	err := run([]string{"--population", "/evidence/population"}, &output, nil,
		func(root string) (lab.PopulationIndex, error) {
			if root != "/evidence/population" {
				t.Fatalf("population root = %q", root)
			}
			return lab.PopulationIndex{Schema: "large-artifact-durability-population-v1", ValidRuns: 36}, nil
		})
	if err != nil {
		t.Fatalf("run population: %v", err)
	}
	if !strings.Contains(output.String(), `"valid_runs": 36`) {
		t.Fatalf("population output = %q", output.String())
	}
}
