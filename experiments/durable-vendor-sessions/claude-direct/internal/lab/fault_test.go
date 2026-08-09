package lab

import "testing"

func TestUnsafeFaultScheduleCoversDeclaredBoundaries(t *testing.T) {
	want := []FaultBoundary{
		FaultBeforeVendorRegistration,
		FaultAfterToolEffect,
		FaultAfterFinalOutput,
	}
	got := unsafeFaultSchedule()
	if len(got) != len(want) {
		t.Fatalf("fault schedule = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("fault schedule[%d] = %q, want %q", index, got[index], want[index])
		}
	}
}
