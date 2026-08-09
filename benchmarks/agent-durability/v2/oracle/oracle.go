// Package oracle independently loads, validates, reconstructs, and scores v2
// evidence. Adapter output can explain a result but cannot select its verdict.
package oracle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func EvaluateAndWrite(ctx context.Context, runDir string) (protocol.Verdict, error) {
	verdict := evaluate(ctx, runDir)
	if err := writeVerdict(ctx, filepath.Join(runDir, protocol.VerdictFile), verdict); err != nil {
		return protocol.Verdict{}, err
	}
	return verdict, nil
}

func evaluate(ctx context.Context, runDir string) protocol.Verdict {
	result := invalidVerdict(protocol.ReasonEvidenceMalformed)
	if err := ctx.Err(); err != nil {
		return result
	}
	loaded, reasons := loadEvidence(runDir)
	result.RunID = loaded.Manifest.RunID
	result.Case = loaded.Manifest.Case
	result.Probe = loaded.Manifest.Probe
	result.Trial = loaded.Manifest.Trial
	if len(reasons) != 0 {
		result.ReasonCodes = uniqueSorted(reasons)
		return result
	}
	if err := loaded.Validate(); err != nil {
		result.ReasonCodes = []string{protocol.ReasonEvidenceMalformed}
		return result
	}
	if !evidenceConsistent(loaded) {
		result.ReasonCodes = []string{protocol.ReasonEvidenceInconsistent}
		return result
	}
	if !casePreconditions(loaded) {
		result.ReasonCodes = []string{protocol.ReasonCasePrecondition}
		return result
	}

	result.Admission = protocol.AdmissionValid
	result.Metrics = calculateMetrics(runDir, loaded)
	result.Correctness, result.Safety, result.Liveness, result.ReasonCodes = score(loaded, result.Metrics)
	result.Diagnosability = protocol.OutcomePass
	if loaded.Manifest.Probe == protocol.ProbeUnsafe && result.Correctness == protocol.OutcomePass &&
		result.Safety == protocol.OutcomePass && result.Liveness == protocol.OutcomePass {
		result.Admission = protocol.AdmissionInvalid
		result.Correctness = protocol.OutcomeNotApplicable
		result.Safety = protocol.OutcomeNotApplicable
		result.Liveness = protocol.OutcomeNotApplicable
		result.Diagnosability = protocol.OutcomeNotApplicable
		result.ReasonCodes = []string{protocol.ReasonNegativeControlWeak}
		return result
	}
	result.ReasonCodes = uniqueSorted(result.ReasonCodes)
	result.EfficiencyEligible = result.Correctness == protocol.OutcomePass && result.Safety == protocol.OutcomePass &&
		result.Liveness == protocol.OutcomePass && result.Diagnosability == protocol.OutcomePass
	return result
}

func invalidVerdict(reason string) protocol.Verdict {
	return protocol.Verdict{
		ContractVersion: protocol.ContractVersion,
		Admission:       protocol.AdmissionInvalid,
		Correctness:     protocol.OutcomeNotApplicable,
		Safety:          protocol.OutcomeNotApplicable,
		Liveness:        protocol.OutcomeNotApplicable,
		Diagnosability:  protocol.OutcomeNotApplicable,
		ReasonCodes:     []string{reason},
		Oracle:          protocol.OracleProtocol,
	}
}

func loadEvidence(runDir string) (protocol.EvidenceBundle, []string) {
	var loaded protocol.EvidenceBundle
	if err := readJSON(filepath.Join(runDir, protocol.ManifestFile), &loaded.Manifest); err != nil {
		return loaded, []string{reasonForReadError(err)}
	}
	if err := loaded.Manifest.Validate(); err != nil {
		return loaded, []string{protocol.ReasonEvidenceMalformed}
	}
	var reasons []string
	for name, expected := range loaded.Manifest.EvidenceSHA256 {
		actual, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			reasons = append(reasons, reasonForReadError(err))
		} else if actual != expected {
			reasons = append(reasons, protocol.ReasonEvidenceHashMismatch)
		}
	}
	if len(reasons) != 0 {
		return loaded, reasons
	}
	readers := []struct {
		name  string
		value any
	}{
		{protocol.AuthorityStateFile, &loaded.Authority},
		{protocol.DestinationStateFile, &loaded.Destination},
		{protocol.DependencyStateFile, &loaded.Dependency},
		{protocol.WorkloadStateFile, &loaded.Workload},
		{protocol.FaultBoundaryFile, &loaded.Fault},
		{protocol.ProcessObservationsFile, &loaded.Processes},
		{protocol.NativeJournalFile, &loaded.Native},
		{protocol.EffectiveInputFile, &loaded.Input},
	}
	for _, reader := range readers {
		if err := readJSON(filepath.Join(runDir, reader.name), reader.value); err != nil {
			reasons = append(reasons, reasonForReadError(err))
		}
	}
	if err := readJSONL(filepath.Join(runDir, protocol.CausalEventsFile), &loaded.Events); err != nil {
		reasons = append(reasons, reasonForReadError(err))
	}
	return loaded, uniqueSorted(reasons)
}

func evidenceConsistent(loaded protocol.EvidenceBundle) bool {
	events := make(map[string]protocol.CausalEvent, len(loaded.Events))
	acceptedEvents := make(map[string]protocol.CausalEvent)
	for _, event := range loaded.Events {
		events[event.EventID] = event
		if event.Decision == protocol.DecisionAccepted &&
			(event.Kind == protocol.EventActionAccepted || event.Kind == protocol.EventOutcomeAccepted || event.Kind == protocol.EventAcknowledged) {
			acceptedEvents[event.EventID] = event
		}
	}
	if len(acceptedEvents) != len(loaded.Authority.AcceptedActions) {
		return false
	}
	for _, action := range loaded.Authority.AcceptedActions {
		event, ok := acceptedEvents[action.EventID]
		if !ok || event.Details["action"] != action.Kind || event.ActorID != action.OwnerID ||
			event.Generation != action.Generation || event.CapabilityHash != action.CapabilityHash {
			return false
		}
	}
	for _, observation := range loaded.Processes {
		event, ok := events[observation.EventID]
		if !ok || event.ActorID != observation.OwnerID || event.Generation != observation.Generation ||
			event.WorkerID != observation.WorkerID || event.ProcessIdentity != observation.ProcessIdentity {
			return false
		}
	}
	for _, item := range loaded.Workload.Items {
		found := false
		for _, event := range loaded.Events {
			if event.WorkItemID == item.WorkItemID && event.LogicalOperationID == item.LogicalOperationID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func casePreconditions(loaded protocol.EvidenceBundle) bool {
	if loaded.Manifest.Case != protocol.CaseABAReacquisition {
		return recoveryCasePreconditions(loaded)
	}
	if loaded.Manifest.Probe == protocol.ProbeUnfaulted {
		return !loaded.Fault.Triggered && len(loaded.Authority.Epochs) == 1
	}
	if loaded.Fault.Point != "g7-delayed-until-g9-current" || len(loaded.Authority.Epochs) != 3 {
		return false
	}
	wantOwners := []string{"A", "B", "A"}
	wantGenerations := []uint64{7, 8, 9}
	for index, epoch := range loaded.Authority.Epochs {
		if epoch.OwnerID != wantOwners[index] || epoch.Generation != wantGenerations[index] {
			return false
		}
	}
	after := loaded.Events[loaded.Fault.AfterSequence-1]
	before := loaded.Events[loaded.Fault.BeforeSequence-1]
	if after.Kind != protocol.EventBarrierReached || after.Generation != 7 || before.Kind != protocol.EventRequestFinished || before.Generation != 7 {
		return false
	}
	generation9BeforeRelease := false
	for _, event := range loaded.Events[loaded.Fault.AfterSequence : loaded.Fault.BeforeSequence-1] {
		if event.Kind == protocol.EventAttemptStarted && event.Generation == 9 && event.ActorID == "A" {
			generation9BeforeRelease = true
		}
	}
	return generation9BeforeRelease
}

func calculateMetrics(runDir string, loaded protocol.EvidenceBundle) protocol.Metrics {
	operations := make(map[string]bool)
	acceptedOutcomes := 0
	staleAccepted := 0
	var firstReady, firstStart, acceptedOutcome, acknowledgement time.Time
	for _, event := range loaded.Events {
		operations[event.LogicalOperationID] = true
		eventTime, _ := time.Parse(time.RFC3339Nano, event.Time)
		if event.Kind == protocol.EventOperationReady && firstReady.IsZero() {
			firstReady = eventTime
		}
		if event.Kind == protocol.EventAttemptStarted && firstStart.IsZero() {
			firstStart = eventTime
		}
		if event.Kind == protocol.EventOutcomeAccepted && event.Decision == protocol.DecisionAccepted {
			acceptedOutcomes++
			acceptedOutcome = eventTime
		}
		if event.Kind == protocol.EventAcknowledged && event.Decision == protocol.DecisionAccepted {
			acknowledgement = eventTime
		}
		if event.Decision == protocol.DecisionAccepted && event.Generation != loaded.Authority.CurrentGeneration &&
			(event.Kind == protocol.EventActionAccepted || event.Kind == protocol.EventOutcomeAccepted || event.Kind == protocol.EventAcknowledged) {
			staleAccepted++
		}
	}
	logicalCount := len(operations)
	requestCount := len(loaded.Dependency.Requests)
	amplification := 0.0
	if logicalCount > 0 {
		amplification = float64(requestCount) / float64(logicalCount)
	}
	metrics := protocol.Metrics{
		LogicalOperationCount: logicalCount, WorkItemCount: len(loaded.Workload.Items), AcceptedOutcomeCount: acceptedOutcomes,
		StaleActionAcceptCount: staleAccepted, PhysicalRequestCount: requestCount, AmplificationFactor: amplification,
		CostUnits: sumCost(loaded.Dependency.Requests), DurableRecordCount: len(loaded.Events) + len(loaded.Native),
	}
	metrics.QueueLatencyMillis = maxQueueLatency(loaded)
	metrics.ExecutionLatencyMillis = durationMillis(firstStart, acceptedOutcome)
	metrics.EndToEndLatencyMillis = maxEndToEndLatency(loaded)
	metrics.HealthyTaskLatencyMillis = maxHealthyLatency(loaded)
	recoveryRequests := retryRequestsOrAll(loaded.Dependency.Requests)
	metrics.PeakQPS = peakQPS(recoveryRequests, 10*time.Millisecond)
	metrics.PeakRetryConcurrency = peakConcurrency(recoveryRequests)
	metrics.RetryDelayMillis = maxRetryDelay(loaded.Events)
	metrics.BacklogIntegralMillis = backlogIntegral(loaded.Events)
	metrics.BacklogDrainP50Millis, metrics.BacklogDrainP90Millis, metrics.BacklogDrainP99Millis = backlogDrain(loaded)
	metrics.ThroughputPerSecond = throughput(loaded.Dependency.Requests)
	metrics.OperatorInterventions = operatorInterventions(loaded.Events)
	metrics.DurableBytes = durableBytes(runDir)
	if loaded.Fault.Triggered {
		triggered, _ := time.Parse(time.RFC3339Nano, loaded.Fault.TriggeredAt)
		metrics.RecoveryLatencyMillis = recoveryLatency(triggered, loaded.Dependency.Requests)
		metrics.DetectionLatencyMillis = detectionLatency(triggered, loaded.Events)
	}
	_ = firstReady
	_ = acknowledgement
	return metrics
}

func recoveryCasePreconditions(loaded protocol.EvidenceBundle) bool {
	if loaded.Manifest.Case.Suite() != protocol.SuiteRecoveryDynamics || len(loaded.Dependency.Transitions) == 0 ||
		loaded.Dependency.Transitions[0].State != protocol.DependencyHealthy {
		return false
	}
	if loaded.Manifest.Probe == protocol.ProbeUnfaulted {
		return !loaded.Fault.Triggered
	}
	wantPoint := map[protocol.CaseID]string{
		protocol.CaseLayeredRetryAmplification: "failure-script-after-first-accepted-request",
		protocol.CaseOutageBacklogRecovery:     "outage-backlog-target-before-restoration",
		protocol.CaseBackpressureOverload:      "offered-load-gate-release",
		protocol.CasePoisonWorkIsolation:       "registered-poison-failure-release",
		protocol.CaseSilentProgress:            "silent-progress-after-first-heartbeat",
	}[loaded.Manifest.Case]
	if wantPoint == "" || loaded.Fault.Point != wantPoint {
		return false
	}
	after := loaded.Events[loaded.Fault.AfterSequence-1]
	before := loaded.Events[loaded.Fault.BeforeSequence-1]
	if after.Kind != protocol.EventFaultCommitted || before.Kind != protocol.EventRecoveryObserved {
		return false
	}
	if loaded.Manifest.Case == protocol.CaseOutageBacklogRecovery {
		states := make([]protocol.DependencyStatus, 0, len(loaded.Dependency.Transitions))
		for _, transition := range loaded.Dependency.Transitions {
			states = append(states, transition.State)
		}
		return len(states) == 3 && states[1] == protocol.DependencyOutage && states[2] == protocol.DependencyRecovering
	}
	return true
}

func score(loaded protocol.EvidenceBundle, metrics protocol.Metrics) (protocol.Outcome, protocol.Outcome, protocol.Outcome, []string) {
	correctness := recoveryCorrectness(loaded)
	safety := protocol.OutcomePass
	liveness := recoveryLiveness(loaded, metrics)
	var reasons []string
	if loaded.Manifest.Case == protocol.CaseABAReacquisition {
		correctness = abaCorrectness(loaded)
		if metrics.StaleActionAcceptCount > 0 {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonStaleActionAccepted)
		}
		if !loaded.Authority.CurrentOwnerAlive {
			liveness = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonCurrentOwnerStopped)
		}
		return correctness, safety, liveness, reasons
	}
	if correctness == protocol.OutcomeFail {
		reasons = append(reasons, protocol.ReasonAcceptedWorkLost)
	}
	switch loaded.Manifest.Case {
	case protocol.CaseLayeredRetryAmplification:
		if metrics.PhysicalRequestCount > 4 {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonRetryBudgetExceeded)
		}
	case protocol.CaseOutageBacklogRecovery:
		if metrics.PeakQPS > 500 || metrics.PeakRetryConcurrency > 4 {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonRecoveryStorm)
		}
	case protocol.CaseBackpressureOverload:
		admitted := 0
		for _, item := range loaded.Workload.Items {
			if item.State != protocol.WorkItemRejected {
				admitted++
			}
		}
		if admitted > 20 || peakConcurrency(loaded.Dependency.Requests) > 8 {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonCapacityExceeded)
		}
	case protocol.CasePoisonWorkIsolation:
		if poisonBudgetExceeded(loaded, 3) {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonPoisonBudgetExceeded)
		}
		if metrics.HealthyTaskLatencyMillis > healthyTaskLatencyBound(loaded) {
			liveness = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonHealthyWorkStarved)
		}
	case protocol.CaseSilentProgress:
		if staleDestinationApplied(loaded) {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonStaleActionAccepted)
		}
		if legitimateWaitRevoked(loaded) {
			safety = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonLegitimateWaitRevoked)
		}
		if loaded.Manifest.Probe != protocol.ProbeUnfaulted && !progressDetected(loaded) {
			liveness = protocol.OutcomeFail
			reasons = append(reasons, protocol.ReasonProgressUndetected)
		}
	}
	return correctness, safety, liveness, reasons
}

func healthyTaskLatencyBound(loaded protocol.EvidenceBundle) int64 {
	const calibrationDefault = int64(50)
	value := loaded.Input.Settings["healthy_latency_bound_ms"]
	if value == "" {
		return calibrationDefault
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		return calibrationDefault
	}
	return parsed
}

func abaCorrectness(loaded protocol.EvidenceBundle) protocol.Outcome {
	if len(loaded.Workload.Items) != 1 || loaded.Workload.Items[0].State != protocol.WorkItemSucceeded {
		return protocol.OutcomeFail
	}
	currentOutcomes := 0
	currentAcknowledgements := 0
	for _, event := range loaded.Events {
		if event.Generation != loaded.Authority.CurrentGeneration || event.Decision != protocol.DecisionAccepted {
			continue
		}
		switch event.Kind {
		case protocol.EventOutcomeAccepted:
			currentOutcomes++
		case protocol.EventAcknowledged:
			currentAcknowledgements++
		}
	}
	if currentOutcomes != 1 || currentAcknowledgements != 1 {
		return protocol.OutcomeFail
	}
	return protocol.OutcomePass
}

func recoveryCorrectness(loaded protocol.EvidenceBundle) protocol.Outcome {
	if loaded.Manifest.Case == protocol.CaseABAReacquisition {
		return abaCorrectness(loaded)
	}
	for _, item := range loaded.Workload.Items {
		switch loaded.Manifest.Case {
		case protocol.CaseBackpressureOverload:
			if item.State != protocol.WorkItemSucceeded && item.State != protocol.WorkItemRejected {
				return protocol.OutcomeFail
			}
		case protocol.CasePoisonWorkIsolation:
			if item.Poison {
				if item.State != protocol.WorkItemSucceeded && item.State != protocol.WorkItemQuarantined {
					return protocol.OutcomeFail
				}
			} else if item.State != protocol.WorkItemSucceeded {
				return protocol.OutcomeFail
			}
		default:
			if item.State != protocol.WorkItemSucceeded {
				return protocol.OutcomeFail
			}
		}
	}
	return protocol.OutcomePass
}

func recoveryLiveness(loaded protocol.EvidenceBundle, metrics protocol.Metrics) protocol.Outcome {
	if loaded.Manifest.Case == protocol.CaseABAReacquisition {
		if loaded.Authority.CurrentOwnerAlive {
			return protocol.OutcomePass
		}
		return protocol.OutcomeFail
	}
	for _, item := range loaded.Workload.Items {
		if item.State == protocol.WorkItemRejected && loaded.Manifest.Case == protocol.CaseBackpressureOverload {
			continue
		}
		if item.Poison && loaded.Manifest.Case == protocol.CasePoisonWorkIsolation && item.State == protocol.WorkItemQuarantined {
			continue
		}
		if item.State != protocol.WorkItemSucceeded {
			return protocol.OutcomeFail
		}
	}
	_ = metrics
	return protocol.OutcomePass
}

func maxQueueLatency(loaded protocol.EvidenceBundle) int64 {
	ready := operationReadyTimes(loaded.Events)
	firstStart := make(map[string]time.Time)
	for _, request := range loaded.Dependency.Requests {
		started, _ := time.Parse(time.RFC3339Nano, request.StartedAt)
		prior, found := firstStart[request.WorkItemID]
		if !found || started.Before(prior) {
			firstStart[request.WorkItemID] = started
		}
	}
	var maximum int64
	for itemID, started := range firstStart {
		if value := durationMillis(ready[itemID], started); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func maxEndToEndLatency(loaded protocol.EvidenceBundle) int64 {
	ready := operationReadyTimes(loaded.Events)
	finish := successfulFinishTimes(loaded.Dependency.Requests)
	for _, event := range loaded.Events {
		if event.Kind == protocol.EventActionRejected && event.Details["action"] == "admission" {
			at, _ := time.Parse(time.RFC3339Nano, event.Time)
			finish[event.WorkItemID] = at
		}
	}
	var maximum int64
	for itemID, finished := range finish {
		if value := durationMillis(ready[itemID], finished); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func maxHealthyLatency(loaded protocol.EvidenceBundle) int64 {
	ready := operationReadyTimes(loaded.Events)
	finish := successfulFinishTimes(loaded.Dependency.Requests)
	healthy := make(map[string]bool, len(loaded.Workload.Items))
	for _, item := range loaded.Workload.Items {
		healthy[item.WorkItemID] = !item.Poison
	}
	var maximum int64
	for itemID, finished := range finish {
		if healthy[itemID] {
			if value := durationMillis(ready[itemID], finished); value > maximum {
				maximum = value
			}
		}
	}
	return maximum
}

func operationReadyTimes(events []protocol.CausalEvent) map[string]time.Time {
	result := make(map[string]time.Time)
	for _, event := range events {
		if event.Kind == protocol.EventOperationReady {
			parsed, _ := time.Parse(time.RFC3339Nano, event.Time)
			if prior, found := result[event.WorkItemID]; !found || parsed.Before(prior) {
				result[event.WorkItemID] = parsed
			}
		}
	}
	return result
}

func successfulFinishTimes(requests []protocol.DependencyRequest) map[string]time.Time {
	result := make(map[string]time.Time)
	for _, request := range requests {
		if request.Outcome != "ok" && request.Outcome != "accepted_then_timeout_script_activated" {
			continue
		}
		finished, _ := time.Parse(time.RFC3339Nano, request.FinishedAt)
		if prior, found := result[request.WorkItemID]; !found || finished.After(prior) {
			result[request.WorkItemID] = finished
		}
	}
	return result
}

func peakQPS(requests []protocol.DependencyRequest, window time.Duration) float64 {
	if len(requests) == 0 || window <= 0 {
		return 0
	}
	starts := make([]time.Time, 0, len(requests))
	for _, request := range requests {
		started, _ := time.Parse(time.RFC3339Nano, request.StartedAt)
		starts = append(starts, started)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	maximum := 0
	for left, right := 0, 0; left < len(starts); left++ {
		if right < left {
			right = left
		}
		for right < len(starts) && starts[right].Before(starts[left].Add(window)) {
			right++
		}
		if right-left > maximum {
			maximum = right - left
		}
	}
	return float64(maximum) / window.Seconds()
}

func retryRequestsOrAll(requests []protocol.DependencyRequest) []protocol.DependencyRequest {
	retries := make([]protocol.DependencyRequest, 0, len(requests))
	for _, request := range requests {
		if request.RetryOrdinal > 1 {
			retries = append(retries, request)
		}
	}
	if len(retries) == 0 {
		return requests
	}
	return retries
}

func peakConcurrency(requests []protocol.DependencyRequest) int {
	type point struct {
		at    time.Time
		delta int
	}
	points := make([]point, 0, len(requests)*2)
	for _, request := range requests {
		started, _ := time.Parse(time.RFC3339Nano, request.StartedAt)
		finished, _ := time.Parse(time.RFC3339Nano, request.FinishedAt)
		points = append(points, point{at: started, delta: 1}, point{at: finished, delta: -1})
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].at.Equal(points[j].at) {
			return points[i].delta < points[j].delta
		}
		return points[i].at.Before(points[j].at)
	})
	current, maximum := 0, 0
	for _, point := range points {
		current += point.delta
		if current > maximum {
			maximum = current
		}
	}
	return maximum
}

func maxRetryDelay(events []protocol.CausalEvent) int64 {
	finished := make(map[string]time.Time)
	var maximum int64
	for _, event := range events {
		at, _ := time.Parse(time.RFC3339Nano, event.Time)
		if event.Kind == protocol.EventAttemptFinished {
			finished[event.AttemptID] = at
		}
		if event.Kind == protocol.EventAttemptStarted && event.ParentAttemptID != "" {
			if value := durationMillis(finished[event.ParentAttemptID], at); value > maximum {
				maximum = value
			}
		}
	}
	return maximum
}

func backlogIntegral(events []protocol.CausalEvent) int64 {
	type sample struct {
		at    time.Time
		depth int64
	}
	var samples []sample
	for _, event := range events {
		value, found := event.Details["queue_depth"]
		if !found {
			continue
		}
		depth, err := strconv.ParseInt(value, 10, 64)
		if err != nil || depth < 0 {
			continue
		}
		at, _ := time.Parse(time.RFC3339Nano, event.Time)
		samples = append(samples, sample{at: at, depth: depth})
	}
	var integral int64
	for index := 0; index+1 < len(samples); index++ {
		integral += samples[index].depth * samples[index+1].at.Sub(samples[index].at).Milliseconds()
	}
	return integral
}

func backlogDrain(loaded protocol.EvidenceBundle) (int64, int64, int64) {
	var restored time.Time
	for _, transition := range loaded.Dependency.Transitions {
		if transition.State == protocol.DependencyRecovering {
			restored, _ = time.Parse(time.RFC3339Nano, transition.Time)
			break
		}
	}
	if restored.IsZero() {
		return 0, 0, 0
	}
	var values []int64
	for _, finish := range successfulFinishTimes(loaded.Dependency.Requests) {
		if !finish.Before(restored) {
			values = append(values, finish.Sub(restored).Milliseconds())
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return percentile(values, 0.50), percentile(values, 0.90), percentile(values, 0.99)
}

func percentile(values []int64, quantile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * quantile)
	return values[index]
}

func throughput(requests []protocol.DependencyRequest) float64 {
	if len(requests) == 0 {
		return 0
	}
	var first, last time.Time
	successes := 0
	for _, request := range requests {
		started, _ := time.Parse(time.RFC3339Nano, request.StartedAt)
		finished, _ := time.Parse(time.RFC3339Nano, request.FinishedAt)
		if first.IsZero() || started.Before(first) {
			first = started
		}
		if last.IsZero() || finished.After(last) {
			last = finished
		}
		if request.Outcome == "ok" {
			successes++
		}
	}
	span := last.Sub(first).Seconds()
	if span <= 0 {
		return float64(successes)
	}
	return float64(successes) / span
}

func operatorInterventions(events []protocol.CausalEvent) int {
	count := 0
	for _, event := range events {
		if event.Details["operator_intervention"] == "true" {
			count++
		}
	}
	return count
}

func durableBytes(runDir string) int64 {
	var total int64
	for _, name := range protocol.RawEvidenceFiles() {
		info, err := os.Stat(filepath.Join(runDir, name))
		if err == nil {
			total += info.Size()
		}
	}
	return total
}

func recoveryLatency(triggered time.Time, requests []protocol.DependencyRequest) int64 {
	var last time.Time
	for _, finish := range successfulFinishTimes(requests) {
		if finish.After(triggered) && finish.After(last) {
			last = finish
		}
	}
	return durationMillis(triggered, last)
}

func detectionLatency(_ time.Time, events []protocol.CausalEvent) int64 {
	var lastProgress time.Time
	for _, event := range events {
		at, _ := time.Parse(time.RFC3339Nano, event.Time)
		if event.Kind == protocol.EventProgressAccepted {
			lastProgress = at
		}
		if event.Kind == protocol.EventRecoveryObserved && event.Decision == protocol.DecisionAccepted && event.Details["detection"] != "" {
			return durationMillis(lastProgress, at)
		}
	}
	return 0
}

func poisonBudgetExceeded(loaded protocol.EvidenceBundle, budget int) bool {
	poisonOperations := make(map[string]bool)
	for _, item := range loaded.Workload.Items {
		if item.Poison {
			poisonOperations[item.LogicalOperationID] = true
		}
	}
	counts := make(map[string]int)
	for _, request := range loaded.Dependency.Requests {
		if poisonOperations[request.LogicalOperationID] {
			counts[request.LogicalOperationID]++
		}
	}
	for _, count := range counts {
		if count > budget {
			return true
		}
	}
	return false
}

func staleDestinationApplied(loaded protocol.EvidenceBundle) bool {
	for _, attempt := range loaded.Destination.Attempts {
		if attempt.Applied && attempt.Generation != loaded.Authority.CurrentGeneration {
			return true
		}
	}
	return false
}

func legitimateWaitRevoked(loaded protocol.EvidenceBundle) bool {
	legitimate := make(map[string]bool)
	for _, event := range loaded.Events {
		if event.Kind == protocol.EventOperationReady && event.Details["role"] == "legitimate-wait" {
			legitimate[event.LogicalOperationID] = true
		}
	}
	for _, event := range loaded.Events {
		if legitimate[event.LogicalOperationID] && event.Kind == protocol.EventActionRejected {
			return true
		}
	}
	return false
}

func progressDetected(loaded protocol.EvidenceBundle) bool {
	for _, event := range loaded.Events {
		if event.Kind == protocol.EventRecoveryObserved && event.Decision == protocol.DecisionAccepted &&
			event.Details["detection"] != "" && event.Details["detection"] != "missing" {
			return true
		}
	}
	return false
}

func sumCost(requests []protocol.DependencyRequest) int64 {
	var total int64
	for _, request := range requests {
		total += request.CostUnits
	}
	return total
}

func durationMillis(start, finish time.Time) int64 {
	if start.IsZero() || finish.IsZero() || finish.Before(start) {
		return 0
	}
	return finish.Sub(start).Milliseconds()
}

func readJSON(path string, destination any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON content")
	}
	return nil
}

func readJSONL(path string, destination *[]protocol.CausalEvent) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	decoder.DisallowUnknownFields()
	for {
		var event protocol.CausalEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		*destination = append(*destination, event)
	}
	return nil
}

func writeVerdict(ctx context.Context, path string, verdict protocol.Verdict) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(verdict, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func reasonForReadError(err error) string {
	if errors.Is(err, os.ErrNotExist) {
		return protocol.ReasonEvidenceMissing
	}
	return protocol.ReasonEvidenceMalformed
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
