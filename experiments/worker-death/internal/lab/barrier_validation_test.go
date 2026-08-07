package lab

import (
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/failureinject"
)

func TestArrivalMatchesExpectedIdentity(t *testing.T) {
	expected := failureinject.Arrival{
		ID: "worker-1:activity-after-launch-decision/1", Point: "activity-after-launch-decision/1",
		SessionID: "session-1", OwnerTokenHash: "owner-hash", Generation: 1, ActorID: "worker-1",
	}
	actual := expected
	actual.Time = time.Now().UTC()
	if !arrivalMatchesExpected(actual, expected) {
		t.Fatal("matching arrival was rejected")
	}

	tests := map[string]func(*failureinject.Arrival){
		"id":            func(arrival *failureinject.Arrival) { arrival.ID = "spoof" },
		"point":         func(arrival *failureinject.Arrival) { arrival.Point = "wrong" },
		"session":       func(arrival *failureinject.Arrival) { arrival.SessionID = "wrong" },
		"owner hash":    func(arrival *failureinject.Arrival) { arrival.OwnerTokenHash = "wrong" },
		"generation":    func(arrival *failureinject.Arrival) { arrival.Generation = 2 },
		"actor":         func(arrival *failureinject.Arrival) { arrival.ActorID = "wrong" },
		"pid":           func(arrival *failureinject.Arrival) { arrival.PID = 42 },
		"process start": func(arrival *failureinject.Arrival) { arrival.ProcessStart = "wrong" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			mismatch := expected
			mutate(&mismatch)
			if arrivalMatchesExpected(mismatch, expected) {
				t.Fatalf("%s mismatch was accepted: %+v", name, mismatch)
			}
		})
	}
}
