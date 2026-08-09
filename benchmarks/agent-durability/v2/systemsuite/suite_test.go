package systemsuite

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestExpectedVerdictEnforcesParityAndDistinguishingUnsafeControl(t *testing.T) {
	base := protocol.Verdict{
		Admission: protocol.AdmissionValid, Correctness: protocol.OutcomePass, Safety: protocol.OutcomePass,
		Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass,
	}
	base.Probe = protocol.ProbeProtected
	if err := expectedVerdict(base); err != nil {
		t.Fatal(err)
	}
	base.Probe = protocol.ProbeUnsafe
	if err := expectedVerdict(base); err == nil {
		t.Fatal("non-distinguishing unsafe verdict accepted")
	}
	base.Safety = protocol.OutcomeFail
	if err := expectedVerdict(base); err != nil {
		t.Fatal(err)
	}
}
