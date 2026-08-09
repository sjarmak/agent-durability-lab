package oracle

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
)

func TestAmbiguousEffectBoundaryAcceptsOnlyDeclaredExactPoints(t *testing.T) {
	tests := []struct {
		boundary string
		point    string
		after    string
	}{
		{
			boundary: protocol.FaultPointProcessCreatedBeforeVendorRegistration,
			point:    protocol.FaultPointProcessCreatedBeforeVendorRegistration, after: protocol.EventBarrierReached,
		},
		{
			boundary: protocol.FaultPointToolEffectBeforeActivityCompletion,
			point:    protocol.FaultPointToolEffectBeforeActivityCompletion, after: protocol.EventBarrierReached,
		},
		{
			boundary: protocol.FaultPointFinalOutputBeforeActivityCompletion,
			point:    protocol.FaultPointFinalOutputBeforeActivityCompletion, after: protocol.EventBarrierReached,
		},
		{boundary: "invented-boundary"},
	}
	for _, test := range tests {
		loaded := evidence{
			manifest: protocol.Manifest{Case: protocol.CaseAmbiguousEffect},
			input:    protocol.EffectiveInput{Settings: map[string]string{"fault_boundary": test.boundary}},
		}
		point, after, _ := expectedBoundary(loaded)
		if point != test.point || after != test.after {
			t.Fatalf("boundary %q = (%q, %q), want (%q, %q)",
				test.boundary, point, after, test.point, test.after)
		}
	}
}
