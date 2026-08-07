package main

import (
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/cancellation/internal/lab"
)

func TestParseScenarios(t *testing.T) {
	all, err := parseScenarios("all")
	if err != nil || len(all) != 4 {
		t.Fatalf("parse all = %v, %v", all, err)
	}
	one, err := parseScenarios("healthy-safe")
	if err != nil || !reflect.DeepEqual(one, []lab.Scenario{lab.ScenarioHealthySafe}) {
		t.Fatalf("parse one = %v, %v", one, err)
	}
	if _, err := parseScenarios("invalid"); err == nil {
		t.Fatal("invalid scenario returned nil error")
	}
}

func TestParseWaitPolicies(t *testing.T) {
	both, err := parseWaitPolicies("both")
	if err != nil || !reflect.DeepEqual(both, []bool{false, true}) {
		t.Fatalf("parse both = %v, %v", both, err)
	}
	if one, err := parseWaitPolicies("true"); err != nil || !reflect.DeepEqual(one, []bool{true}) {
		t.Fatalf("parse true = %v, %v", one, err)
	}
	if _, err := parseWaitPolicies("invalid"); err == nil {
		t.Fatal("invalid wait policy returned nil error")
	}
}
