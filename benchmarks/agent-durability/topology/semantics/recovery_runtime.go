package semantics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

const (
	protectedRequestsPerItemMax  = 4
	protectedRetryConcurrencyMax = 2
	protectedPoisonAttemptsMax   = 3
	progressDeadlineMS           = 5000
	workerActivityConcurrency    = 8
)

type recoveryItemRuntime struct {
	role            string
	poison          bool
	admitted        bool
	disposition     string
	scheduleEventID string
	startEventID    string
	terminalEventID string
	attempts        map[int]bool
	processes       map[string]bool
	acceptedEffects int
	acceptedResults int
	costUnits       int64
	scheduledOffset int64
	startedOffset   int64
	terminalOffset  int64
}

type recoveryRuntimeState struct {
	items map[string]*recoveryItemRuntime

	retryTokens chan struct{}

	admissionBatches    map[int]recoveryAdmissionRecord
	admittedOutstanding int
	peakAdmitted        int
	terminalAccounted   map[string]bool

	backpressureReady       int
	backpressureRelease     chan struct{}
	backpressureReleaseOnce sync.Once

	poisonInitialStarted map[string]bool
	poisonRelease        chan struct{}
	poisonReleaseOnce    sync.Once

	retryBoundaryReady chan struct{}
	retryBoundaryOnce  sync.Once

	outageCoordinatorReady chan struct{}
	outageCoordinatorOnce  sync.Once
	outageInitial          map[string]bool
	outageRestoreOnce      sync.Once
	outageRestored         chan struct{}
	outageCrashOnce        sync.Once
	outageStartedOffset    int64
	outageRestoredOffset   int64
	outageIsRestored       bool

	lastRequestByItem  map[string]protocol.DependencyRequest
	requestCountByItem map[string]int
	requestGates       map[string]chan struct{}
	replacementGates   map[string]chan struct{}

	wedgeLeaseByItem         map[string]recoveryWedge
	recordedBySession        map[string]recoverySnapshotCount
	falsePositiveRevocations int
	staleActionAccepts       int
	progressEventID          string
	deadlineEventID          string
	detectionEventID         string
	replacementEventID       string

	dependency *recoveryDependencyService
}

type recoveryAdmissionRecord struct {
	receipt RecoveryAdmissionReceipt
	items   []string
}

type recoveryWedge struct {
	identity          protocol.Identity
	leaseGeneration   uint64
	beforeEffectPoint string
}

type recoverySnapshotCount struct {
	effects int
	results int
}

func newRecoveryRuntimeState(spec EpisodeSpec) (*recoveryRuntimeState, error) {
	service, err := startRecoveryDependencyService(spec.Case)
	if err != nil {
		return nil, err
	}
	state := &recoveryRuntimeState{
		items:               make(map[string]*recoveryItemRuntime, spec.Fanout),
		retryTokens:         make(chan struct{}, protectedRetryConcurrencyMax),
		admissionBatches:    make(map[int]recoveryAdmissionRecord),
		terminalAccounted:   make(map[string]bool, spec.Fanout),
		backpressureRelease: make(chan struct{}), poisonRelease: make(chan struct{}),
		poisonInitialStarted: make(map[string]bool, spec.Fanout), retryBoundaryReady: make(chan struct{}),
		outageCoordinatorReady: make(chan struct{}), outageRestored: make(chan struct{}),
		outageInitial:      make(map[string]bool, spec.Fanout),
		lastRequestByItem:  make(map[string]protocol.DependencyRequest, spec.Fanout),
		requestCountByItem: make(map[string]int, spec.Fanout), requestGates: make(map[string]chan struct{}, spec.Fanout),
		replacementGates:  make(map[string]chan struct{}, spec.Fanout),
		wedgeLeaseByItem:  make(map[string]recoveryWedge),
		recordedBySession: make(map[string]recoverySnapshotCount, spec.Fanout),
		dependency:        service,
	}
	for ordinal := 1; ordinal <= spec.Fanout; ordinal++ {
		itemID := fmt.Sprintf("item-%03d", ordinal)
		role := "healthy"
		poison := spec.Case == protocol.CasePoisonWorkIsolation && ordinal == 1
		if poison {
			role = "poison"
		}
		if spec.Case == protocol.CaseSilentProgress {
			switch ordinal {
			case 1:
				role = "wedged"
			case 2:
				role = "declared-wait"
			}
		}
		state.items[itemID] = &recoveryItemRuntime{
			role: role, poison: poison, attempts: make(map[int]bool), processes: make(map[string]bool),
		}
		state.requestGates[itemID] = make(chan struct{}, 1)
		state.replacementGates[itemID] = make(chan struct{}, 1)
	}
	return state, nil
}

func (r *EpisodeRuntime) beginRecoveryActivity(ctx context.Context) error {
	if r.recovery == nil {
		return fmt.Errorf("%w: missing recovery runtime", protocol.ErrInvalidEvidence)
	}
	r.beginActivity()
	return nil
}

func (r *EpisodeRuntime) endRecoveryActivity() {
	r.endActivity()
}

func (r *EpisodeRuntime) recordRecoveryActivityStart(identity protocol.Identity, startedID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.recovery.items[identity.WorkItemID]
	item.attempts[identity.ActivityAttempt] = true
	if item.startEventID == "" {
		item.startEventID = startedID
		item.startedOffset = r.events[len(r.events)-1].MonotonicOffsetNS
	}
}

func (r *EpisodeRuntime) recoveryItems() []protocol.RecoveryItemObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]protocol.RecoveryItemObservation, 0, len(r.recovery.items))
	for itemID, item := range r.recovery.items {
		result = append(result, protocol.RecoveryItemObservation{
			WorkItemID: itemID, Role: item.role, Poison: item.poison, Admitted: item.admitted,
			Disposition: item.disposition, ScheduleEventID: item.scheduleEventID, StartEventID: item.startEventID,
			TerminalEventID: item.terminalEventID, ActivityAttempts: len(item.attempts), AgentProcesses: len(item.processes),
			AcceptedEffects: item.acceptedEffects, AcceptedResults: item.acceptedResults, CostUnits: item.costUnits,
		})
	}
	slices.SortFunc(result, func(first, second protocol.RecoveryItemObservation) int {
		if first.WorkItemID < second.WorkItemID {
			return -1
		}
		if first.WorkItemID > second.WorkItemID {
			return 1
		}
		return 0
	})
	return result
}

type dependencyServiceRequest struct {
	RequestID string          `json:"request_id"`
	ItemID    string          `json:"item_id"`
	Ordinal   int             `json:"ordinal"`
	Case      protocol.CaseID `json:"case"`
	Probe     protocol.Probe  `json:"probe"`
	Fanout    int             `json:"fanout"`
}

type dependencyServiceResponse struct {
	Outcome           string `json:"outcome"`
	CostUnits         int64  `json:"cost_units"`
	ConcurrentAtStart int    `json:"concurrent_at_start"`
}

type dependencyServiceObservation struct {
	requestID  string
	started    time.Time
	finished   time.Time
	concurrent int
	outcome    string
}

type recoveryDependencyService struct {
	server   *http.Server
	listener net.Listener
	url      string
	done     chan error

	mu                    sync.Mutex
	outage                bool
	active                int
	peak                  int
	observations          []dependencyServiceObservation
	unsafeCatchupArrivals int
	unsafeCatchupRelease  chan struct{}
	unsafeCatchupOnce     sync.Once
}

func startRecoveryDependencyService(benchmarkCase protocol.CaseID) (*recoveryDependencyService, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	service := &recoveryDependencyService{
		listener: listener, url: "http://" + listener.Addr().String(), done: make(chan error, 1),
		unsafeCatchupRelease: make(chan struct{}),
	}
	if benchmarkCase == protocol.CaseOutageBacklogHerdRecovery {
		service.outage = false
	}
	service.server = &http.Server{
		Handler: http.HandlerFunc(service.handle), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second,
	}
	go func() {
		serveErr := service.server.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		service.done <- serveErr
	}()
	return service, nil
}

func (s *recoveryDependencyService) handle(writer http.ResponseWriter, request *http.Request) {
	defer func() { _ = request.Body.Close() }()
	request.Body = http.MaxBytesReader(writer, request.Body, 64<<10)
	var input dependencyServiceRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&input)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if request.Method != http.MethodPost || request.URL.Path != "/" || decodeErr != nil || trailingErr != io.EOF ||
		input.RequestID == "" || input.ItemID == "" || input.Ordinal < 1 || !input.Case.Valid() ||
		!input.Probe.Valid() || input.Fanout < 1 {
		http.Error(writer, "invalid dependency request", http.StatusBadRequest)
		return
	}
	started := time.Now().UTC()
	s.mu.Lock()
	s.active++
	concurrent := s.active
	if concurrent > s.peak {
		s.peak = concurrent
	}
	outage := s.outage
	s.mu.Unlock()

	outcome := "ok"
	switch input.Case {
	case protocol.CaseLayeredRetryAmplification:
		outcome = []string{"timeout", "500", "429", "ok"}[(input.Ordinal-1)%4]
	case protocol.CaseOutageBacklogHerdRecovery:
		if outage {
			outcome = "outage"
		} else if input.Probe == protocol.ProbeUnsafe && input.Ordinal > 1 {
			s.mu.Lock()
			s.unsafeCatchupArrivals++
			threshold := input.Fanout
			if threshold > workerActivityConcurrency {
				threshold = workerActivityConcurrency
			}
			release := s.unsafeCatchupRelease
			if s.unsafeCatchupArrivals == threshold {
				s.unsafeCatchupOnce.Do(func() { close(release) })
			}
			s.mu.Unlock()
			select {
			case <-request.Context().Done():
			case <-release:
			}
		}
	case protocol.CasePoisonWorkIsolation:
		if input.ItemID == "item-001" {
			outcome = "permanent_failure"
		}
	}
	finished := time.Now().UTC()
	s.mu.Lock()
	s.active--
	s.observations = append(s.observations, dependencyServiceObservation{
		requestID: input.RequestID, started: started, finished: finished, concurrent: concurrent, outcome: outcome,
	})
	s.mu.Unlock()
	response := dependencyServiceResponse{Outcome: outcome, CostUnits: 1, ConcurrentAtStart: concurrent}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

func (s *recoveryDependencyService) setOutage(value bool) {
	s.mu.Lock()
	s.outage = value
	s.mu.Unlock()
}

func (s *recoveryDependencyService) close() error {
	if s == nil {
		return nil
	}
	// Every Activity has reached a terminal disposition before runtime teardown;
	// force-closing any pooled idle HTTP connection avoids making evidence
	// admission depend on net/http keep-alive timing.
	return errors.Join(s.server.Close(), <-s.done)
}

func (r *EpisodeRuntime) callRecoveryDependency(
	ctx context.Context,
	identity protocol.Identity,
	owner string,
) (dependencyServiceResponse, error) {
	gate := r.recovery.requestGates[identity.WorkItemID]
	if gate == nil {
		return dependencyServiceResponse{}, fmt.Errorf("%w: dependency request gate for %s", protocol.ErrInvalidEvidence, identity.WorkItemID)
	}
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return dependencyServiceResponse{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return dependencyServiceResponse{}, err
	}
	r.mu.Lock()
	ordinal := r.recovery.requestCountByItem[identity.WorkItemID] + 1
	r.recovery.requestCountByItem[identity.WorkItemID] = ordinal
	previous := r.recovery.lastRequestByItem[identity.WorkItemID]
	requestID := fmt.Sprintf("%s/%s/request-%03d", r.spec.RunID, identity.WorkItemID, ordinal)
	startedOffset := time.Since(r.startedAt).Nanoseconds()
	retryDelay := int64(0)
	if previous.RequestID != "" {
		retryDelay = (startedOffset - previous.FinishedOffsetNS) / int64(time.Millisecond)
		if retryDelay < 0 {
			retryDelay = 0
		}
	}
	r.mu.Unlock()
	startedID := r.appendEvent(identity, protocol.EventDependencyStarted, protocol.DecisionObserved, map[string]string{
		"request_id": requestID, "retry_owner": owner, "retry_ordinal": fmt.Sprint(ordinal),
	})
	payload, err := json.Marshal(dependencyServiceRequest{
		RequestID: requestID, ItemID: identity.WorkItemID, Ordinal: ordinal, Case: r.spec.Case,
		Probe: r.spec.Probe, Fanout: r.spec.Fanout,
	})
	if err != nil {
		return dependencyServiceResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, r.recovery.dependency.url, bytes.NewReader(payload))
	if err != nil {
		return dependencyServiceResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, callErr := http.DefaultClient.Do(httpRequest)
	result := dependencyServiceResponse{Outcome: "transport_error", CostUnits: 1, ConcurrentAtStart: 1}
	if callErr == nil {
		decodeErr := json.NewDecoder(response.Body).Decode(&result)
		closeErr := response.Body.Close()
		callErr = errors.Join(decodeErr, closeErr)
	}
	finishedOffset := time.Since(r.startedAt).Nanoseconds()
	decision := protocol.DecisionAccepted
	if result.Outcome != "ok" {
		decision = protocol.DecisionFailed
	}
	finishedID := r.appendEvent(identity, protocol.EventDependencyFinished, decision, map[string]string{
		"request_id": requestID, "outcome": result.Outcome,
	}, startedID)
	serviceMS := (finishedOffset - startedOffset) / int64(time.Millisecond)
	if serviceMS < 0 {
		serviceMS = 0
	}
	record := protocol.DependencyRequest{
		RequestID: requestID, EventID: finishedID, StartedEventID: startedID, ParentRequestID: previous.RequestID,
		WorkItemID: identity.WorkItemID, Attempt: identity.ActivityAttempt, RetryOrdinal: ordinal, RetryOwner: owner,
		Outcome: result.Outcome, CostUnits: result.CostUnits, StartedOffsetNS: startedOffset, FinishedOffsetNS: finishedOffset,
		RetryDelayMS: retryDelay, ServiceMS: serviceMS, ConcurrentAtStart: result.ConcurrentAtStart,
	}
	r.mu.Lock()
	r.requests = append(r.requests, record)
	r.recovery.lastRequestByItem[identity.WorkItemID] = record
	r.recovery.items[identity.WorkItemID].costUnits += result.CostUnits
	r.mu.Unlock()
	if callErr != nil {
		return result, callErr
	}
	return result, nil
}
