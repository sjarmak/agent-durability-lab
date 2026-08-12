package lab

import "testing"

func TestFaultSchedulesCoverMatchedAndFencedBoundaries(t *testing.T) {
	unsafe := unsafeFaultSchedule()
	fenced := fencedFaultSchedule()
	if len(unsafe) != 3 || unsafe[0] != FaultBeforeThreadObservation ||
		unsafe[1] != FaultAfterToolEffect || unsafe[2] != FaultAfterFinalOutput {
		t.Fatalf("unsafe schedule = %v", unsafe)
	}
	if len(fenced) != 8 || fenced[0] != FaultAfterClaimBeforeExec ||
		fenced[1] != FaultBeforeThreadObservation || fenced[2] != FaultAfterThreadBeforeRegistration ||
		fenced[3] != FaultAfterToolEffect || fenced[4] != FaultAfterFinalOutput ||
		fenced[5] != FaultConcurrentRecovery || fenced[6] != FaultCancellationWhileExecuting ||
		fenced[7] != FaultProcessFailureReplacement {
		t.Fatalf("fenced schedule = %v", fenced)
	}
	if point := faultBarrierPoint(FaultBoundary("unsupported")); point != "" {
		t.Fatalf("unsupported fault barrier point = %q", point)
	}
}
