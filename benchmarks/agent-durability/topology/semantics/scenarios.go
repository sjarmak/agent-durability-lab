package semantics

import "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"

// Scenario is one mechanism-level semantics case/boundary/probe combination.
// Scale strata and publication slots are added by the frozen schedule later.
type Scenario struct {
	Case     protocol.CaseID
	Boundary string
	Probe    protocol.Probe
}

// FrozenSemanticsScenarios covers every primary and secondary boundary for the
// four orchestration-semantics cases. Each unsafe boundary is an executable
// negative control, not merely a label on the protected implementation.
func FrozenSemanticsScenarios() []Scenario {
	return []Scenario{
		{protocol.CaseJoinBarrier, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted},
		{protocol.CaseJoinBarrier, "designated-item-result-observed-before-activity-completion", protocol.ProbeUnsafe},
		{protocol.CaseJoinBarrier, "designated-item-result-observed-before-activity-completion", protocol.ProbeProtected},
		{protocol.CaseJoinBarrier, "required-item-terminal-failure-before-join", protocol.ProbeUnsafe},
		{protocol.CaseJoinBarrier, "required-item-terminal-failure-before-join", protocol.ProbeProtected},
		{protocol.CaseIncrementalPartialReduction, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted},
		{protocol.CaseIncrementalPartialReduction, "partial-checkpoint-accepted-before-checkpoint-activity-completion", protocol.ProbeUnsafe},
		{protocol.CaseIncrementalPartialReduction, "partial-checkpoint-accepted-before-checkpoint-activity-completion", protocol.ProbeProtected},
		{protocol.CaseIncrementalPartialReduction, "contribution-observed-before-work-activity-completion", protocol.ProbeUnsafe},
		{protocol.CaseIncrementalPartialReduction, "contribution-observed-before-work-activity-completion", protocol.ProbeProtected},
		{protocol.CaseQueuedExecutingSupersession, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted},
		{protocol.CaseQueuedExecutingSupersession, "executing-after-process-start-before-effect", protocol.ProbeUnsafe},
		{protocol.CaseQueuedExecutingSupersession, "executing-after-process-start-before-effect", protocol.ProbeProtected},
		{protocol.CaseQueuedExecutingSupersession, "queued-before-activity-start", protocol.ProbeUnsafe},
		{protocol.CaseQueuedExecutingSupersession, "queued-before-activity-start", protocol.ProbeProtected},
		{protocol.CaseDestructiveTransition, protocol.UnfaultedBoundary, protocol.ProbeUnfaulted},
		{protocol.CaseDestructiveTransition, "destination-accepted-before-activity-completion", protocol.ProbeUnsafe},
		{protocol.CaseDestructiveTransition, "destination-accepted-before-activity-completion", protocol.ProbeProtected},
		{protocol.CaseDestructiveTransition, "before-destination-acceptance", protocol.ProbeUnsafe},
		{protocol.CaseDestructiveTransition, "before-destination-acceptance", protocol.ProbeProtected},
		{protocol.CaseDestructiveTransition, "activity-result-recorded-before-outcome-acknowledgement", protocol.ProbeUnsafe},
		{protocol.CaseDestructiveTransition, "activity-result-recorded-before-outcome-acknowledgement", protocol.ProbeProtected},
	}
}

// FrozenRecoveryScenarios covers the six primary recovery mechanisms plus all
// four secondary claim-to-acknowledgement crash boundaries. Each faulted
// mechanism executes both its unsafe control and protected implementation.
func FrozenRecoveryScenarios() []Scenario {
	result := make([]Scenario, 0, 26)
	primary := []Scenario{
		{protocol.CaseCrashRecoveryBoundaries, "result-observed-before-activity-completion", ""},
		{protocol.CaseLayeredRetryAmplification, "dependency-first-request-before-scripted-timeout-500-429-sequence", ""},
		{protocol.CaseOutageBacklogHerdRecovery, "outage-backlog-restoration-and-catchup-worker-crash", ""},
		{protocol.CaseBackpressureOverload, "ready-workers-before-fixed-cohort-release", ""},
		{protocol.CasePoisonWorkIsolation, "mixed-cohort-admitted-before-poison-failure-release", ""},
		{protocol.CaseSilentProgress, "accepted-progress-before-executor-wedge", ""},
	}
	for _, scenario := range primary {
		result = append(result, Scenario{Case: scenario.Case, Boundary: protocol.UnfaultedBoundary, Probe: protocol.ProbeUnfaulted})
		result = append(result,
			Scenario{Case: scenario.Case, Boundary: scenario.Boundary, Probe: protocol.ProbeUnsafe},
			Scenario{Case: scenario.Case, Boundary: scenario.Boundary, Probe: protocol.ProbeProtected},
		)
	}
	for _, boundary := range []string{
		"claim-accepted-before-process-launch",
		"process-launched-before-durable-registration",
		"checkpoint-accepted-before-activity-completion",
		"parent-outcome-recorded-before-acknowledgement",
	} {
		result = append(result,
			Scenario{Case: protocol.CaseCrashRecoveryBoundaries, Boundary: boundary, Probe: protocol.ProbeUnsafe},
			Scenario{Case: protocol.CaseCrashRecoveryBoundaries, Boundary: boundary, Probe: protocol.ProbeProtected},
		)
	}
	return result
}
