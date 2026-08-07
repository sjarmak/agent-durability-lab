package lab

import (
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestVerifyTemporalControlRequiresObservedPostCancelMutation(t *testing.T) {
	snapshot := workstore.Snapshot{
		Executors: []workstore.Executor{{Status: workstore.ExecutorStatusCompleted}},
		Effects:   []workstore.AcceptedEffect{{Effect: workstore.Effect{ID: "effect"}}},
		Outcome:   &workstore.Outcome{Value: "done"},
	}
	verdict := Verify(ScenarioTemporalControl, snapshot)
	if !verdict.RunValid || !verdict.ExpectedObservation || verdict.InvariantSatisfied {
		t.Fatalf("control verdict = %+v", verdict)
	}

	snapshot.Outcome = nil
	verdict = Verify(ScenarioTemporalControl, snapshot)
	if verdict.RunValid || verdict.ExpectedObservation {
		t.Fatalf("invalid control verdict = %+v", verdict)
	}
}

func TestVerifySafeCancellationRequiresRevocationAckAndProcessTreeEvidence(t *testing.T) {
	events := []workstore.Event{}
	for _, kind := range []string{
		"cancellation_committed", "executor_stop_delivery_attempted", "executor_stop_delivery_sent",
		"executor_stop_received", "cancellation_acknowledged", "tool_child_registered", "tool_child_stop_received",
	} {
		events = append(events, workstore.Event{Kind: kind})
	}
	snapshot := workstore.Snapshot{
		Executors: []workstore.Executor{{Status: workstore.ExecutorStatusCanceled}},
		Cancellation: &workstore.Cancellation{
			Acknowledgement: &workstore.CancellationAcknowledgement{AcknowledgedAt: time.Now()},
		},
		Events: events,
	}
	verdict := Verify(ScenarioHealthySafe, snapshot)
	if !verdict.RunValid || !verdict.ExpectedObservation || !verdict.InvariantSatisfied {
		t.Fatalf("safe verdict = %+v", verdict)
	}

	snapshot.Effects = []workstore.AcceptedEffect{{Effect: workstore.Effect{ID: "late"}}}
	verdict = Verify(ScenarioHealthySafe, snapshot)
	if verdict.ExpectedObservation || verdict.InvariantSatisfied {
		t.Fatalf("unsafe post-cancel verdict = %+v", verdict)
	}
}

func TestVerifyScenarioSpecificEvidence(t *testing.T) {
	snapshot := workstore.Snapshot{
		Executors: []workstore.Executor{{Status: workstore.ExecutorStatusCanceled}},
		Cancellation: &workstore.Cancellation{
			Acknowledgement: &workstore.CancellationAcknowledgement{AcknowledgedAt: time.Now()},
		},
	}
	for _, kind := range []string{
		"cancellation_committed", "executor_stop_delivery_attempted", "executor_stop_delivery_sent",
		"executor_stop_received", "cancellation_acknowledged", "tool_child_registered", "tool_child_stop_received",
	} {
		snapshot.Events = append(snapshot.Events, workstore.Event{Kind: kind})
	}
	if verdict := Verify(ScenarioWorkerDeathSafe, snapshot); verdict.RunValid {
		t.Fatalf("worker-death verdict without kill evidence = %+v", verdict)
	}
	if verdict := Verify(ScenarioFrozenSafe, snapshot); verdict.RunValid {
		t.Fatalf("frozen verdict without freeze/resume evidence = %+v", verdict)
	}
	if verdict := Verify("unknown", snapshot); verdict.RunValid {
		t.Fatalf("unknown scenario verdict = %+v", verdict)
	}
}

func TestVerifyRejectsIncompleteControlAndSafeEvidence(t *testing.T) {
	t.Run("control with application cancellation", func(t *testing.T) {
		snapshot := workstore.Snapshot{
			Executors:    []workstore.Executor{{Status: workstore.ExecutorStatusCompleted}},
			Effects:      []workstore.AcceptedEffect{{Effect: workstore.Effect{ID: "effect"}}},
			Outcome:      &workstore.Outcome{Value: "done"},
			Cancellation: &workstore.Cancellation{},
		}
		verdict := Verify(ScenarioTemporalControl, snapshot)
		if verdict.RunValid || verdict.ExpectedObservation || verdict.InvariantSatisfied {
			t.Fatalf("control verdict = %+v; want invalid evidence", verdict)
		}
	})

	t.Run("safe without cancellation", func(t *testing.T) {
		verdict := Verify(ScenarioHealthySafe, workstore.Snapshot{
			Executors: []workstore.Executor{{Status: workstore.ExecutorStatusRunning}},
		})
		if verdict.RunValid || verdict.ExpectedObservation || verdict.InvariantSatisfied {
			t.Fatalf("safe verdict = %+v; want missing cancellation failure", verdict)
		}
	})

	t.Run("safe with missing acknowledgement events and canceled status", func(t *testing.T) {
		verdict := Verify(ScenarioHealthySafe, workstore.Snapshot{
			Executors:    []workstore.Executor{{Status: workstore.ExecutorStatusRunning}},
			Effects:      []workstore.AcceptedEffect{{Effect: workstore.Effect{ID: "late"}}},
			Outcome:      &workstore.Outcome{Value: "late"},
			Cancellation: &workstore.Cancellation{},
		})
		if verdict.ExpectedObservation || verdict.InvariantSatisfied || len(verdict.Failures) < 4 {
			t.Fatalf("safe verdict = %+v; want independent evidence failures", verdict)
		}
	})
}
