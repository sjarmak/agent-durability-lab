package main

import (
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/experiments/activity-completion-identity/internal/lab"
)

func TestParseArms(t *testing.T) {
	tests := []struct {
		input   string
		want    []lab.Arm
		wantErr bool
	}{
		{
			input: "all",
			want:  []lab.Arm{lab.ArmStaleTaskToken, lab.ArmStaleByID, lab.ArmFencedByID},
		},
		{input: "STALE-BY-ID", want: []lab.Arm{lab.ArmStaleByID}},
		{input: "unknown", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseArms(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseArms(%q) error = %v; wantErr %v", test.input, err, test.wantErr)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("parseArms(%q) = %v; want %v", test.input, got, test.want)
			}
		})
	}
}
