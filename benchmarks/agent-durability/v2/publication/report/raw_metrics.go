// Package report reconstructs publication estimands from append-only raw
// evidence. It is intentionally outside the frozen execution harness.
package report

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

type PrimaryMetrics map[string]float64

func ReconstructPrimaryMetrics(runDir string, estimands []string) (PrimaryMetrics, error) {
	bundle, err := loadBundle(runDir)
	if err != nil {
		return nil, err
	}
	all, err := reconstruct(bundle, runDir)
	if err != nil {
		return nil, err
	}
	result := make(PrimaryMetrics, len(estimands))
	for _, estimand := range estimands {
		value, ok := all[estimand]
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("%w: primary estimand %q is not reconstructible", protocol.ErrInvalidEvidence, estimand)
		}
		result[estimand] = value
	}
	return result, nil
}

func reconstruct(bundle protocol.EvidenceBundle, runDir string) (PrimaryMetrics, error) {
	ready := make(map[string]time.Time)
	firstStart := make(map[string]time.Time)
	finish := make(map[string]time.Time)
	operations := make(map[string]bool)
	roles := make(map[string]string)
	finishedAttempts := make(map[string]time.Time)
	var lastProgress time.Time
	var detection int64
	staleAccepted, legitimateWaitRevoked := 0, 0
	for _, event := range bundle.Events {
		at, err := parseTime(event.Time)
		if err != nil {
			return nil, err
		}
		if event.LogicalOperationID != "" {
			operations[event.LogicalOperationID] = true
		}
		switch event.Kind {
		case protocol.EventOperationReady:
			if prior, ok := ready[event.WorkItemID]; !ok || at.Before(prior) {
				ready[event.WorkItemID] = at
			}
			if event.Details["role"] != "" {
				roles[event.WorkItemID] = event.Details["role"]
			}
		case protocol.EventAttemptFinished:
			finishedAttempts[event.AttemptID] = at
		case protocol.EventProgressAccepted:
			lastProgress = at
		case protocol.EventRecoveryObserved:
			if event.Decision == protocol.DecisionAccepted && event.Details["detection"] != "" {
				detection = millis(lastProgress, at)
			}
		}
		if event.Decision == protocol.DecisionAccepted && event.Generation != bundle.Authority.CurrentGeneration &&
			(event.Kind == protocol.EventActionAccepted || event.Kind == protocol.EventOutcomeAccepted || event.Kind == protocol.EventAcknowledged) {
			staleAccepted++
		}
		if event.Kind == protocol.EventActionRejected && roles[event.WorkItemID] == "legitimate-wait" {
			legitimateWaitRevoked++
		}
	}

	activeNanoseconds := int64(0)
	for _, request := range bundle.Dependency.Requests {
		started, err := parseTime(request.StartedAt)
		if err != nil {
			return nil, err
		}
		finished, err := parseTime(request.FinishedAt)
		if err != nil {
			return nil, err
		}
		if prior, ok := firstStart[request.WorkItemID]; !ok || started.Before(prior) {
			firstStart[request.WorkItemID] = started
		}
		activeNanoseconds += finished.Sub(started).Nanoseconds()
		if request.Outcome == "ok" || request.Outcome == "accepted" || request.Outcome == "accepted_then_timeout_script_activated" {
			if prior, ok := finish[request.WorkItemID]; !ok || finished.After(prior) {
				finish[request.WorkItemID] = finished
			}
		}
	}
	for _, event := range bundle.Events {
		if event.Kind == protocol.EventActionRejected && event.Details["action"] == "admission" {
			at, err := parseTime(event.Time)
			if err != nil {
				return nil, err
			}
			finish[event.WorkItemID] = at
		}
	}

	queueLatency := maxByItem(ready, firstStart)
	endToEnd := maxByItem(ready, finish)
	healthy := make(map[string]bool, len(bundle.Workload.Items))
	rejected := 0
	for _, item := range bundle.Workload.Items {
		healthy[item.WorkItemID] = !item.Poison
		if item.State == protocol.WorkItemRejected {
			rejected++
		}
	}
	healthyLatency := int64(0)
	for itemID, finished := range finish {
		if healthy[itemID] && millis(ready[itemID], finished) > healthyLatency {
			healthyLatency = millis(ready[itemID], finished)
		}
	}

	physical := len(bundle.Dependency.Requests)
	amplification := float64(0)
	if len(operations) > 0 {
		amplification = float64(physical) / float64(len(operations))
	}
	recovery := int64(0)
	if bundle.Fault.Triggered {
		triggered, err := parseTime(bundle.Fault.TriggeredAt)
		if err != nil {
			return nil, err
		}
		var last time.Time
		for _, finished := range finish {
			if finished.After(triggered) && finished.After(last) {
				last = finished
			}
		}
		recovery = millis(triggered, last)
	}
	backlogIntegral, drainP90, err := backlogMetrics(bundle)
	if err != nil {
		return nil, err
	}
	durableBytes, err := rawBytes(runDir)
	if err != nil {
		return nil, err
	}
	admissionFraction := float64(0)
	if bundle.Workload.ExpectedWorkItems > 0 {
		admissionFraction = float64(rejected) / float64(bundle.Workload.ExpectedWorkItems)
	}

	return PrimaryMetrics{
		"stale_action_accept_count":        float64(staleAccepted),
		"physical_request_count":           float64(physical),
		"amplification_factor":             amplification,
		"peak_qps":                         peakQPS(retryRequestsOrAll(bundle.Dependency.Requests), 10*time.Millisecond),
		"peak_retry_concurrency":           float64(peakConcurrency(retryRequestsOrAll(bundle.Dependency.Requests))),
		"queue_latency_ms":                 float64(queueLatency),
		"execution_latency_ms":             float64(activeNanoseconds / int64(time.Millisecond)),
		"failure_detection_latency_ms":     float64(detection),
		"retry_delay_ms":                   float64(retryDelay(bundle.Events, finishedAttempts)),
		"recovery_latency_ms":              float64(recovery),
		"end_to_end_latency_ms":            float64(endToEnd),
		"backlog_integral_ms":              float64(backlogIntegral),
		"backlog_drain_p90_ms":             float64(drainP90),
		"healthy_task_latency_ms":          float64(healthyLatency),
		"throughput_per_second":            throughput(bundle.Dependency.Requests),
		"admission_rejection_fraction":     admissionFraction,
		"legitimate_wait_revocation_count": float64(legitimateWaitRevoked),
		"cost_units":                       float64(sumCost(bundle.Dependency.Requests)),
		"durable_record_count":             float64(len(bundle.Events) + len(bundle.Native)),
		"durable_bytes":                    float64(durableBytes),
		"operator_intervention_count":      float64(operatorInterventions(bundle.Events)),
	}, nil
}

func loadBundle(runDir string) (protocol.EvidenceBundle, error) {
	var bundle protocol.EvidenceBundle
	if err := readJSON(filepath.Join(runDir, protocol.ManifestFile), &bundle.Manifest); err != nil {
		return bundle, err
	}
	if err := bundle.Manifest.Validate(); err != nil {
		return bundle, err
	}
	for name, expected := range bundle.Manifest.EvidenceSHA256 {
		actual, err := protocol.FileSHA256(filepath.Join(runDir, name))
		if err != nil {
			return bundle, err
		}
		if actual != expected {
			return bundle, fmt.Errorf("%w: raw hash mismatch for %s", protocol.ErrInvalidEvidence, name)
		}
	}
	readers := []struct {
		name  string
		value any
	}{
		{protocol.AuthorityStateFile, &bundle.Authority},
		{protocol.DestinationStateFile, &bundle.Destination},
		{protocol.DependencyStateFile, &bundle.Dependency},
		{protocol.WorkloadStateFile, &bundle.Workload},
		{protocol.FaultBoundaryFile, &bundle.Fault},
		{protocol.ProcessObservationsFile, &bundle.Processes},
		{protocol.NativeJournalFile, &bundle.Native},
		{protocol.EffectiveInputFile, &bundle.Input},
	}
	for _, reader := range readers {
		if err := readJSON(filepath.Join(runDir, reader.name), reader.value); err != nil {
			return bundle, err
		}
	}
	if err := readJSONL(filepath.Join(runDir, protocol.CausalEventsFile), &bundle.Events); err != nil {
		return bundle, err
	}
	if err := bundle.Validate(); err != nil {
		return bundle, err
	}
	return bundle, nil
}

func backlogMetrics(bundle protocol.EvidenceBundle) (int64, int64, error) {
	type point struct {
		at    time.Time
		delta int64
	}
	failed := make(map[string]time.Time)
	for _, request := range bundle.Dependency.Requests {
		if request.Outcome != "outage" {
			continue
		}
		finished, err := parseTime(request.FinishedAt)
		if err != nil {
			return 0, 0, err
		}
		if prior, ok := failed[request.WorkItemID]; !ok || finished.Before(prior) {
			failed[request.WorkItemID] = finished
		}
	}
	if len(failed) == 0 {
		return 0, 0, nil
	}
	points := make([]point, 0, len(failed)*2)
	completed := make(map[string]time.Time, len(failed))
	for itemID, at := range failed {
		points = append(points, point{at: at, delta: 1})
		for _, request := range bundle.Dependency.Requests {
			if request.WorkItemID != itemID || request.Outcome != "ok" {
				continue
			}
			finished, err := parseTime(request.FinishedAt)
			if err != nil {
				return 0, 0, err
			}
			if finished.Before(at) {
				continue
			}
			if prior, ok := completed[itemID]; !ok || finished.Before(prior) {
				completed[itemID] = finished
			}
		}
	}
	if len(completed) != len(failed) {
		return 0, 0, fmt.Errorf("%w: outage backlog did not fully drain", protocol.ErrInvalidEvidence)
	}
	for _, at := range completed {
		points = append(points, point{at: at, delta: -1})
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].at.Equal(points[j].at) {
			return points[i].delta > points[j].delta
		}
		return points[i].at.Before(points[j].at)
	})
	depth := int64(0)
	previous := points[0].at
	areaNanoseconds := int64(0)
	for _, current := range points {
		areaNanoseconds += depth * current.at.Sub(previous).Nanoseconds()
		depth += current.delta
		if depth < 0 {
			return 0, 0, fmt.Errorf("%w: negative reconstructed backlog", protocol.ErrInvalidEvidence)
		}
		previous = current.at
	}
	if depth != 0 {
		return 0, 0, fmt.Errorf("%w: residual reconstructed backlog", protocol.ErrInvalidEvidence)
	}
	restored := time.Time{}
	for _, transition := range bundle.Dependency.Transitions {
		if transition.State == protocol.DependencyRecovering {
			var err error
			restored, err = parseTime(transition.Time)
			if err != nil {
				return 0, 0, err
			}
			break
		}
	}
	if restored.IsZero() {
		return 0, 0, fmt.Errorf("%w: outage backlog lacks restoration anchor", protocol.ErrInvalidEvidence)
	}
	drains := make([]int64, 0, len(completed))
	for _, at := range completed {
		drains = append(drains, millis(restored, at))
	}
	sort.Slice(drains, func(i, j int) bool { return drains[i] < drains[j] })
	return areaNanoseconds / int64(time.Millisecond), percentile(drains, 0.90), nil
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

func peakQPS(requests []protocol.DependencyRequest, window time.Duration) float64 {
	if len(requests) == 0 {
		return 0
	}
	starts := make([]time.Time, 0, len(requests))
	for _, request := range requests {
		at, _ := parseTime(request.StartedAt)
		starts = append(starts, at)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	maximum := 0
	for left, right := 0, 0; left < len(starts); left++ {
		for right < len(starts) && starts[right].Before(starts[left].Add(window)) {
			right++
		}
		if right-left > maximum {
			maximum = right - left
		}
	}
	return float64(maximum) / window.Seconds()
}

func peakConcurrency(requests []protocol.DependencyRequest) int {
	type point struct {
		at    time.Time
		delta int
	}
	points := make([]point, 0, len(requests)*2)
	for _, request := range requests {
		started, _ := parseTime(request.StartedAt)
		finished, _ := parseTime(request.FinishedAt)
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

func retryDelay(events []protocol.CausalEvent, finished map[string]time.Time) int64 {
	maximum := int64(0)
	for _, event := range events {
		if event.Kind != protocol.EventAttemptStarted || event.ParentAttemptID == "" {
			continue
		}
		at, _ := parseTime(event.Time)
		if value := millis(finished[event.ParentAttemptID], at); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func throughput(requests []protocol.DependencyRequest) float64 {
	if len(requests) == 0 {
		return 0
	}
	var first, last time.Time
	successes := 0
	for _, request := range requests {
		started, _ := parseTime(request.StartedAt)
		finished, _ := parseTime(request.FinishedAt)
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
	if seconds := last.Sub(first).Seconds(); seconds > 0 {
		return float64(successes) / seconds
	}
	return float64(successes)
}

func maxByItem(start, finish map[string]time.Time) int64 {
	maximum := int64(0)
	for itemID, finished := range finish {
		if value := millis(start[itemID], finished); value > maximum {
			maximum = value
		}
	}
	return maximum
}

func percentile(values []int64, quantile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	return values[int(float64(len(values)-1)*quantile)]
}

func sumCost(requests []protocol.DependencyRequest) int64 {
	var total int64
	for _, request := range requests {
		total += request.CostUnits
	}
	return total
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

func rawBytes(runDir string) (int64, error) {
	var total int64
	for _, name := range protocol.RawEvidenceFiles() {
		info, err := os.Stat(filepath.Join(runDir, name))
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

func millis(start, finish time.Time) int64 {
	if start.IsZero() || finish.IsZero() || finish.Before(start) {
		return 0
	}
	return finish.Sub(start).Milliseconds()
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: timestamp %q", protocol.ErrInvalidEvidence, value)
	}
	return parsed, nil
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON in %s", protocol.ErrInvalidEvidence, path)
	}
	return nil
}

func readJSONL(path string, target *[]protocol.CausalEvent) (returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var event protocol.CausalEvent
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return err
		}
		*target = append(*target, event)
	}
	return scanner.Err()
}
