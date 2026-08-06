package main

import (
	"reflect"
	"testing"

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
