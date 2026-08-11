package lab

import (
	"fmt"
	"testing"
)

func TestSupervisorConcurrencySensitiveScenariosRepeatThreeTrials(t *testing.T) {
	scenarios := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "concurrent-launch-attach", run: testTurnSupervisorConcurrentCallersLaunchOnce},
		{name: "replacement-fences-stale", run: testTurnSupervisorReplacementFencesOldEffectAndCompletion},
		{name: "failed-execution-auto-replacement", run: testTurnSupervisorRedeliveryReplacesFailedExecutionWithoutManualFlag},
		{name: "cancel-revokes-before-signal", run: testTurnSupervisorCancellationRevokesBeforeSignalingExecution},
		{name: "aba-effect-fence", run: testSupervisorHTTPEffectFenceRejectsDelayedAuthorityAfterABA},
		{name: "real-process-cancellation", run: testRunSupervisedInvocationCancellationTargetsExactRegisteredProcess},
		{name: "real-process-replacement", run: testTurnSupervisorReplacementStopsExactOldProcessAfterFenceCommit},
	}
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			for trial := 1; trial <= 3; trial++ {
				t.Run(fmt.Sprintf("trial-%d", trial), scenario.run)
			}
		})
	}
}
