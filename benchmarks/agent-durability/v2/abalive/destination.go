package abalive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type authority struct {
	owner      string
	generation uint64
	capability string
}

type barrierArrival struct {
	event     protocol.CausalEvent
	requestID string
}

type destination struct {
	mu          sync.Mutex
	probe       protocol.Probe
	runID       string
	operationID string
	workItemID  string
	current     authority
	recorder    *recorder
	barrier     chan barrierArrival
	release     chan struct{}
	seen        map[string]bool
	releaseOnce sync.Once
}

func newDestination(probe protocol.Probe, recorder *recorder) *destination {
	return &destination{
		probe: probe, runID: recorder.runID, operationID: recorder.operationID, workItemID: recorder.workItemID,
		recorder: recorder, barrier: make(chan barrierArrival, 1), release: make(chan struct{}), seen: make(map[string]bool),
	}
}

func (d *destination) setAuthority(owner string, generation uint64, capability string) {
	d.mu.Lock()
	d.current = authority{owner: owner, generation: generation, capability: capability}
	d.mu.Unlock()
	d.recorder.ownerChanged(owner, generation, hashCapability(capability))
}

func (d *destination) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/actions", d.handleAction)
	return mux
}

func (d *destination) handleAction(response http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(response, request.Body, 64<<10)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var action ActionRequest
	if err := decoder.Decode(&action); err != nil {
		http.Error(response, "invalid action", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		http.Error(response, "invalid trailing action data", http.StatusBadRequest)
		return
	}
	if err := d.validate(action); err != nil {
		http.Error(response, err.Error(), http.StatusConflict)
		return
	}
	start := d.recorder.requestStarted(action)
	parent := start.EventID
	if action.OwnerID == "A" && action.Generation == 7 && action.Action == ActionEffect {
		barrier := d.recorder.barrier(action, start.EventID)
		parent = barrier.EventID
		select {
		case d.barrier <- barrierArrival{event: barrier, requestID: action.RequestID}:
		default:
			http.Error(response, "duplicate delayed request", http.StatusConflict)
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-d.release:
		}
	}
	accepted, reason := d.authorize(action)
	d.recorder.finish(action, accepted, reason, parent)
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(ActionResponse{RequestID: action.RequestID, Action: action.Action, Accepted: accepted, Reason: reason})
}

func (d *destination) validate(action ActionRequest) error {
	if action.RunID != d.runID || action.LogicalOperationID != d.operationID || action.WorkItemID != d.workItemID ||
		action.OwnerID == "" || action.Generation == 0 || action.Capability == "" || action.AttemptID == "" ||
		action.RetryOrdinal < 1 || action.WorkerID == "" || action.ProcessIdentity == "" || action.RequestID == "" || !action.Action.valid() {
		return fmt.Errorf("%w: action identity is incomplete or wrong", protocol.ErrInvalidEvidence)
	}
	if action.RetryOrdinal == 1 && action.ParentAttemptID != "" || action.RetryOrdinal > 1 && (action.ParentAttemptID == "" || action.RetryCause == "") {
		return fmt.Errorf("%w: retry identity is inconsistent", protocol.ErrInvalidEvidence)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[action.RequestID] {
		return fmt.Errorf("%w: request ID was reused", protocol.ErrInvalidEvidence)
	}
	d.seen[action.RequestID] = true
	return nil
}

func (d *destination) authorize(action ActionRequest) (bool, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	accepted := action.OwnerID == d.current.owner
	reason := "owner_label_match"
	if d.probe == protocol.ProbeProtected {
		accepted = accepted && action.Generation == d.current.generation && action.Capability == d.current.capability
		reason = "current_generation_capability"
	}
	if !accepted {
		return false, "stale_authority"
	}
	if action.Action == ActionStop {
		return true, "current_owner_stopped_by_accepted_request"
	}
	return true, reason
}

func (d *destination) waitForBarrier(ctx context.Context) (barrierArrival, error) {
	select {
	case <-ctx.Done():
		return barrierArrival{}, ctx.Err()
	case arrival := <-d.barrier:
		return arrival, nil
	}
}

func (d *destination) releaseBarrier() {
	d.releaseOnce.Do(func() { close(d.release) })
}

func hashCapability(value string) string {
	return workstore.HashToken(value)
}
