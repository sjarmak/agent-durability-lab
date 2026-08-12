package coverprofile

import (
	"bytes"
	"strings"
	"testing"
)

func TestMergeAddsCountsAndSortsBlocks(t *testing.T) {
	var output bytes.Buffer
	err := Merge(&output, []NamedReader{
		{Name: "default", Reader: strings.NewReader("mode: atomic\nb.go:2.1,2.2 1 0\na.go:1.1,1.2 2 1\n")},
		{Name: "explicit", Reader: strings.NewReader("mode: atomic\na.go:1.1,1.2 2 3\nc.go:3.1,3.2 1 1\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "mode: atomic\na.go:1.1,1.2 2 4\nb.go:2.1,2.2 1 0\nc.go:3.1,3.2 1 1\n"
	if output.String() != want {
		t.Fatalf("merged profile = %q, want %q", output.String(), want)
	}
}

func TestMergeRejectsModeAndStatementDrift(t *testing.T) {
	tests := []struct {
		name     string
		profiles []NamedReader
		want     string
	}{
		{
			name: "mode",
			profiles: []NamedReader{
				{Name: "one", Reader: strings.NewReader("mode: atomic\na.go:1.1,1.2 1 1\n")},
				{Name: "two", Reader: strings.NewReader("mode: set\na.go:1.1,1.2 1 1\n")},
			},
			want: "coverage mode",
		},
		{
			name: "statements",
			profiles: []NamedReader{
				{Name: "one", Reader: strings.NewReader("mode: atomic\na.go:1.1,1.2 1 1\n")},
				{Name: "two", Reader: strings.NewReader("mode: atomic\na.go:1.1,1.2 2 1\n")},
			},
			want: "statement count",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Merge(&output, test.profiles)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Merge error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestMergeRejectsSharedFileBlockLayoutDrift(t *testing.T) {
	tests := []struct {
		name   string
		second string
	}{
		{
			name: "shifted-coordinate",
			second: "mode: atomic\n" +
				"a.go:1.1,1.2 1 1\n" +
				"a.go:3.1,3.2 1 1\n",
		},
		{
			name:   "missing-block",
			second: "mode: atomic\na.go:1.1,1.2 1 1\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Merge(&output, []NamedReader{
				{Name: "current", Reader: strings.NewReader("mode: atomic\na.go:1.1,1.2 1 0\na.go:2.1,2.2 1 0\n")},
				{Name: "stale", Reader: strings.NewReader(test.second)},
			})
			if err == nil || !strings.Contains(err.Error(), "block layout") {
				t.Fatalf("Merge error = %v, want block-layout rejection", err)
			}
		})
	}
}

func TestMergeRejectsMalformedAndDuplicateBlockGeometry(t *testing.T) {
	tests := []struct {
		name    string
		profile string
	}{
		{name: "not-geometry", profile: "mode: atomic\na.go:not-geometry 1 1\n"},
		{name: "zero-position", profile: "mode: atomic\na.go:0.0,1.2 1 1\n"},
		{name: "zero-column", profile: "mode: atomic\na.go:1.0,1.2 1 1\n"},
		{name: "reversed", profile: "mode: atomic\na.go:2.2,1.1 1 1\n"},
		{name: "empty-range", profile: "mode: atomic\na.go:1.1,1.1 1 1\n"},
		{name: "trailing-data", profile: "mode: atomic\na.go:1.1,1.2:extra 1 1\n"},
		{name: "negative", profile: "mode: atomic\na.go:-1.1,1.2 1 1\n"},
		{name: "zero-statements", profile: "mode: atomic\na.go:1.1,1.2 0 1\n"},
		{name: "invalid-mode", profile: "mode: invented\na.go:1.1,1.2 1 1\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Merge(&output, []NamedReader{{Name: test.name, Reader: strings.NewReader(test.profile)}})
			if err == nil {
				t.Fatalf("Merge accepted malformed profile %q", test.profile)
			}
		})
	}
}

func TestMergeAddsRepeatedBlocksFromAggregateGoProfile(t *testing.T) {
	var output bytes.Buffer
	profile := "mode: atomic\na.go:1.1,1.2 1 2\na.go:1.1,1.2 1 3\n"
	if err := Merge(&output, []NamedReader{{Name: "aggregate", Reader: strings.NewReader(profile)}}); err != nil {
		t.Fatalf("Merge rejected repeated aggregate block: %v", err)
	}
	want := "mode: atomic\na.go:1.1,1.2 1 5\n"
	if output.String() != want {
		t.Fatalf("merged profile = %q, want %q", output.String(), want)
	}
}

func TestMergeAcceptsCompilerGeneratedZeroStatementBlock(t *testing.T) {
	var output bytes.Buffer
	profile := "mode: atomic\na.go:1.1,1.1 0 3\na.go:2.1,2.2 1 1\n"
	if err := Merge(&output, []NamedReader{{Name: "compiler", Reader: strings.NewReader(profile)}}); err != nil {
		t.Fatalf("Merge rejected compiler-generated zero-statement block: %v", err)
	}
	if output.String() != profile {
		t.Fatalf("merged profile = %q, want %q", output.String(), profile)
	}
}
