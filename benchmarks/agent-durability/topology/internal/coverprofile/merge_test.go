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
