package lab

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestSupervisorHTTPCallerLossReattachesWithoutRelaunch(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	decisions := make(chan supervisorDecision, 2)
	var runs atomic.Int32
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			runs.Add(1)
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return supervisedResult{}, ctx.Err()
			}
			if err := store.CommitEffect(ctx, lease, workstore.Effect{ID: "effect-1", Value: "one"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				VendorSessionID: "vendor-session-1", PhysicalAttemptID: "attempt-1",
				ProcessIdentity: "pid:10:start:boot:1", Outcome: workstore.Outcome{Value: "done"},
			}, nil
		}, sequentialCapabilities(), withSupervisorDecisionObserver(func(decision supervisorDecision) {
			decisions <- decision
		}))
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, server.Client())

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := client.StartOrAttach(firstContext, supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", AgentBuild: "build-1", Attempt: 1,
		})
		firstResult <- err
	}()
	if decision := <-decisions; decision.Action != workstore.ActionLaunch {
		t.Fatalf("first decision = %+v, want launch", decision)
	}
	<-started
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("lost HTTP caller = %v, want context.Canceled", err)
	}

	attachedResult := make(chan struct {
		receipt supervisorReceipt
		err     error
	}, 1)
	go func() {
		receipt, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-2", AgentBuild: "build-1", Attempt: 2,
		})
		attachedResult <- struct {
			receipt supervisorReceipt
			err     error
		}{receipt: receipt, err: err}
	}()
	if decision := <-decisions; decision.Action != workstore.ActionAttach {
		t.Fatalf("recovery decision = %+v, want attach", decision)
	}
	close(release)
	result := <-attachedResult
	if result.err != nil {
		t.Fatalf("reattach over HTTP: %v", result.err)
	}
	if result.receipt.Action != workstore.ActionAttach || result.receipt.Generation != 1 ||
		result.receipt.OwnerTokenHash != workstore.HashToken("capability-1") ||
		result.receipt.VendorSessionID != "vendor-session-1" || result.receipt.Outcome.Value != "done" {
		t.Fatalf("reattach receipt = %+v", result.receipt)
	}
	if runs.Load() != 1 {
		t.Fatalf("supervised runs = %d, want 1", runs.Load())
	}
}

func TestSupervisorHTTPRejectsMalformedOrUnexpectedRequests(t *testing.T) {
	store := openSupervisorTestStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.New("must not run")
		}, sequentialCapabilities())
	handler := newSupervisorHandler(supervisor)

	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "method", method: http.MethodGet, path: supervisorStartPath, wantStatus: http.StatusMethodNotAllowed},
		{name: "content type", method: http.MethodPost, path: supervisorStartPath, contentType: "text/plain", body: `{}`, wantStatus: http.StatusUnsupportedMediaType},
		{name: "unknown field", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"s","worker_id":"w","attempt":1,"owner_token":"leak"}`, wantStatus: http.StatusBadRequest},
		{name: "trailing object", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"s","worker_id":"w","attempt":1}{}`, wantStatus: http.StatusBadRequest},
		{name: "invalid request", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"","worker_id":"w","attempt":1}`, wantStatus: http.StatusBadRequest},
		{name: "embedded control", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"s\u0000x","worker_id":"w","attempt":1}`, wantStatus: http.StatusBadRequest},
		{name: "oversized logical identity", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"s","worker_id":"w","attempt":1,"logical_turn_id":"` + strings.Repeat("x", supervisorMaxIdentifierBytes+1) + `"}`, wantStatus: http.StatusBadRequest},
		{name: "oversized", method: http.MethodPost, path: supervisorStartPath, contentType: "application/json", body: `{"session_id":"s","worker_id":"w","attempt":1,"agent_build":"` + strings.Repeat("x", supervisorMaxRequestBytes) + `"}`, wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d body=%q, want %d", response.Code, response.Body.String(), test.wantStatus)
			}
			if strings.Contains(response.Body.String(), "owner_token") || strings.Contains(response.Body.String(), "leak") {
				t.Fatalf("error response leaks request details: %q", response.Body.String())
			}
		})
	}

	healthRequest := httptest.NewRequest(http.MethodGet, supervisorHealthPath, nil)
	healthResponse := httptest.NewRecorder()
	handler.ServeHTTP(healthResponse, healthRequest)
	if healthResponse.Code != http.StatusNoContent {
		t.Fatalf("health status = %d, want %d", healthResponse.Code, http.StatusNoContent)
	}
}

func TestSupervisorHTTPCancelCommitsBeforeSignalAndMapsTerminalError(t *testing.T) {
	store := openSupervisorTestStore(t)
	started := make(chan struct{})
	lateEffect := make(chan error, 1)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, supervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			close(started)
			<-ctx.Done()
			snapshot, err := store.Snapshot(context.Background(), lease.SessionID)
			if err != nil {
				return supervisedResult{}, err
			}
			if snapshot.Cancellation == nil {
				return supervisedResult{}, errors.New("execution signaled before durable revocation")
			}
			err = store.CommitEffect(context.Background(), lease, workstore.Effect{ID: "late", Value: "late"})
			lateEffect <- err
			return supervisedResult{}, err
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, server.Client())

	result := make(chan error, 1)
	go func() {
		_, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
			SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
		})
		result <- err
	}()
	<-started
	receipt, err := client.Cancel(context.Background(), supervisorCancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	})
	if err != nil {
		t.Fatalf("cancel over HTTP: %v", err)
	}
	if receipt.Action != workstore.CancelActionCommitted || receipt.Generation != 1 ||
		receipt.OwnerTokenHash != workstore.HashToken("capability-1") {
		t.Fatalf("cancel receipt = %+v", receipt)
	}
	if err := <-lateEffect; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("late effect = %v, want ErrSessionCanceled", err)
	}
	if err := <-result; !errors.Is(err, workstore.ErrSessionCanceled) {
		t.Fatalf("start response error = %v, want ErrSessionCanceled", err)
	}
}

func TestSupervisorHTTPClientMapsStaleOwnerWithoutLeakingWireDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusConflict)
		_, _ = fmt.Fprint(response, `{"code":"stale_owner","message":"request rejected"}`)
	}))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, server.Client())

	_, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
	})
	if !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("mapped error = %v, want ErrStaleOwner", err)
	}
	if strings.Contains(fmt.Sprint(err), "owner_token") {
		t.Fatalf("mapped error leaks capability detail: %v", err)
	}
}

func TestSupervisorHTTPRejectsInvalidCancelAndEffectRequests(t *testing.T) {
	store := openSupervisorTestStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.New("must not run")
		}, sequentialCapabilities())
	handler := newSupervisorHandler(supervisor)
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{name: "health method", method: http.MethodPost, path: supervisorHealthPath, wantStatus: http.StatusMethodNotAllowed},
		{name: "cancel malformed", method: http.MethodPost, path: supervisorCancelPath, body: `{`, wantStatus: http.StatusBadRequest},
		{name: "cancel invalid", method: http.MethodPost, path: supervisorCancelPath, body: `{"session_id":"","request_id":"cancel-1"}`, wantStatus: http.StatusBadRequest},
		{name: "cancel missing session", method: http.MethodPost, path: supervisorCancelPath, body: `{"session_id":"missing","request_id":"cancel-1"}`, wantStatus: http.StatusNotFound},
		{name: "effect malformed", method: http.MethodPost, path: supervisorEffectPath, body: `{`, wantStatus: http.StatusBadRequest},
		{name: "effect invalid", method: http.MethodPost, path: supervisorEffectPath, body: `{"session_id":"session-1"}`, wantStatus: http.StatusBadRequest},
		{name: "effect missing session", method: http.MethodPost, path: supervisorEffectPath, body: `{"session_id":"missing","generation":1,"owner_capability":"capability","effect_id":"effect-1","value":"value"}`, wantStatus: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d body=%q, want %d", response.Code, response.Body.String(), test.wantStatus)
			}
		})
	}
}

func TestSupervisorHTTPMapsEveryWireErrorWithoutDetails(t *testing.T) {
	tests := []struct {
		code       string
		status     int
		want       error
		wantStatus int
	}{
		{code: "invalid_request", status: http.StatusBadRequest, want: workstore.ErrInvalidRequest, wantStatus: http.StatusBadRequest},
		{code: "session_not_found", status: http.StatusNotFound, want: workstore.ErrSessionNotFound, wantStatus: http.StatusNotFound},
		{code: "stale_owner", status: http.StatusConflict, want: workstore.ErrStaleOwner, wantStatus: http.StatusConflict},
		{code: "session_canceled", status: http.StatusConflict, want: workstore.ErrSessionCanceled, wantStatus: http.StatusConflict},
		{code: "effect_conflict", status: http.StatusConflict, want: workstore.ErrEffectConflict, wantStatus: http.StatusConflict},
		{code: "execution_unavailable", status: http.StatusServiceUnavailable, want: errSupervisorExecutionUnavailable, wantStatus: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			mapped := mapSupervisorWireError(test.status, test.code)
			if !errors.Is(mapped, test.want) {
				t.Fatalf("mapped error = %v, want %v", mapped, test.want)
			}
			response := httptest.NewRecorder()
			writeSupervisorOperationError(response, test.want)
			if response.Code != test.wantStatus || strings.Contains(response.Body.String(), test.want.Error()) {
				t.Fatalf("operation response = %d %q", response.Code, response.Body.String())
			}
		})
	}
	for _, test := range []struct {
		err        error
		wantStatus int
	}{
		{err: context.Canceled, wantStatus: http.StatusRequestTimeout},
		{err: context.DeadlineExceeded, wantStatus: http.StatusRequestTimeout},
		{err: errors.New("internal"), wantStatus: http.StatusInternalServerError},
	} {
		response := httptest.NewRecorder()
		writeSupervisorOperationError(response, test.err)
		if response.Code != test.wantStatus {
			t.Fatalf("operation error %v status = %d, want %d", test.err, response.Code, test.wantStatus)
		}
	}
	if err := mapSupervisorWireError(http.StatusTeapot, "unknown"); err == nil || strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unknown wire mapping = %v", err)
	}
}

func TestSupervisorHTTPClientRejectsNonLoopbackAuthorityBeforeTransport(t *testing.T) {
	tests := []string{
		"http://example.com:8080",
		"http://127.0.0.1:8080/path",
		"http://user@127.0.0.1:8080",
		"http://127.0.0.1:8080?query=1",
		"file:///tmp/supervisor.sock",
		"http://localhost:8080",
	}
	for _, baseURL := range tests {
		t.Run(baseURL, func(t *testing.T) {
			transported := atomic.Bool{}
			client := newSupervisorClient(baseURL, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				transported.Store(true)
				return nil, errors.New("must not transport")
			})})
			_, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
				SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
			})
			if !errors.Is(err, workstore.ErrInvalidRequest) || transported.Load() {
				t.Fatalf("base URL %q: error=%v transported=%t", baseURL, err, transported.Load())
			}
		})
	}
}

func TestSupervisorHTTPClientRejectsUnboundedOrNonStrictResponses(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
	}{
		{name: "wrong media type", contentType: "text/plain", body: `{}`},
		{name: "unknown field", contentType: "application/json", body: `{"action":"attach","generation":1,"owner_token_hash":"hash","unknown":true}`},
		{name: "trailing value", contentType: "application/json", body: `{"action":"attach","generation":1,"owner_token_hash":"hash"}{}`},
		{name: "oversized", contentType: "application/json", body: `{"action":"attach","generation":1,"owner_token_hash":"` + strings.Repeat("x", supervisorMaxResponseBytes) + `"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.contentType)
				_, _ = fmt.Fprint(response, test.body)
			}))
			t.Cleanup(server.Close)
			client := newSupervisorClient(server.URL, server.Client())
			_, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
				SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
			})
			if err == nil {
				t.Fatal("non-strict response unexpectedly accepted")
			}
		})
	}
}

func TestSupervisorHTTPClientRejectsSemanticallyInvalidReceipts(t *testing.T) {
	validStart := supervisorReceipt{
		Action: workstore.ActionAttach, Generation: 1, OwnerTokenHash: workstore.HashToken("capability"),
		VendorSessionID: "vendor-session", PhysicalAttemptID: "attempt-1",
		ProcessIdentity: "pid:10:start:boot:1", Outcome: workstore.Outcome{Value: "done"},
	}
	if err := validateSupervisorStartReceipt(validStart); err != nil {
		t.Fatalf("valid start receipt: %v", err)
	}
	startTests := []struct {
		name   string
		change func(*supervisorReceipt)
	}{
		{"unknown action", func(receipt *supervisorReceipt) { receipt.Action = "unknown" }},
		{"zero generation", func(receipt *supervisorReceipt) { receipt.Generation = 0 }},
		{"invalid capability hash", func(receipt *supervisorReceipt) { receipt.OwnerTokenHash = "hash" }},
		{"missing process identity", func(receipt *supervisorReceipt) { receipt.ProcessIdentity = "" }},
		{"missing outcome", func(receipt *supervisorReceipt) { receipt.Outcome = workstore.Outcome{} }},
	}
	for _, test := range startTests {
		t.Run("start "+test.name, func(t *testing.T) {
			receipt := validStart
			test.change(&receipt)
			if err := validateSupervisorStartReceipt(receipt); err == nil {
				t.Fatal("invalid start receipt returned nil error")
			}
		})
	}

	validCancel := supervisorCancelReceipt{
		Action: workstore.CancelActionCommitted, Generation: 1, OwnerTokenHash: workstore.HashToken("capability"),
	}
	if err := validateSupervisorCancelReceipt(validCancel); err != nil {
		t.Fatalf("valid cancel receipt: %v", err)
	}
	for _, receipt := range []supervisorCancelReceipt{
		{Action: "unknown"},
		{Action: workstore.CancelActionCommitted},
		{Action: workstore.CancelActionAlreadyCompleted, Generation: 1},
	} {
		if err := validateSupervisorCancelReceipt(receipt); err == nil {
			t.Fatalf("invalid cancel receipt %+v returned nil error", receipt)
		}
	}
}

func TestSupervisorHTTPClientDoesNotFollowRedirects(t *testing.T) {
	redirected := atomic.Bool{}
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Store(true)
	}))
	t.Cleanup(target.Close)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, server.Client())
	_, err := client.StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
	})
	if err == nil || redirected.Load() {
		t.Fatalf("redirect error=%v followed=%t", err, redirected.Load())
	}
}

func testSupervisorHTTPEffectFenceRejectsDelayedAuthorityAfterABA(t *testing.T) {
	store := openSupervisorTestStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.New("unused runner")
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, server.Client())

	first := claimSupervisorTestGeneration(t, store, "session-1", "worker-a", "old-a", 1, false)
	second := claimSupervisorTestGeneration(t, store, "session-1", "worker-b", "owner-b", 2, true)
	current := claimSupervisorTestGeneration(t, store, "session-1", "worker-a", "new-a", 3, true)
	if first.Generation != 1 || second.Generation != 2 || current.Generation != 3 {
		t.Fatalf("ABA generations = %d/%d/%d", first.Generation, second.Generation, current.Generation)
	}

	err := client.CommitEffect(context.Background(), supervisorEffectRequest{
		SessionID: first.SessionID, Generation: first.Generation, OwnerCapability: first.OwnerToken,
		EffectID: "effect-1", Value: "delayed-old-a",
	})
	if !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("delayed generation-1 effect = %v, want ErrStaleOwner", err)
	}
	if strings.Contains(fmt.Sprint(err), first.OwnerToken) {
		t.Fatalf("stale response leaks owner capability: %v", err)
	}
	if err := client.CommitEffect(context.Background(), supervisorEffectRequest{
		SessionID: current.SessionID, Generation: current.Generation, OwnerCapability: current.OwnerToken,
		EffectID: "effect-1", Value: "current-a",
	}); err != nil {
		t.Fatalf("current generation-3 effect: %v", err)
	}
	if err := client.CommitEffect(context.Background(), supervisorEffectRequest{
		SessionID: current.SessionID, Generation: current.Generation, OwnerCapability: current.OwnerToken,
		EffectID: "effect-1", Value: "current-a",
	}); err != nil {
		t.Fatalf("idempotent generation-3 effect: %v", err)
	}
	if err := client.CommitEffect(context.Background(), supervisorEffectRequest{
		SessionID: current.SessionID, Generation: current.Generation, OwnerCapability: current.OwnerToken,
		EffectID: "effect-1", Value: "conflicting-a",
	}); !errors.Is(err, workstore.ErrEffectConflict) {
		t.Fatalf("conflicting generation-3 effect = %v, want ErrEffectConflict", err)
	}

	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ActiveGeneration != 3 || len(snapshot.Effects) != 1 ||
		snapshot.Effects[0].Generation != 3 || snapshot.Effects[0].Value != "current-a" {
		t.Fatalf("ABA snapshot = %+v", snapshot)
	}
}

func claimSupervisorTestGeneration(t *testing.T, store *workstore.Store, sessionID, workerID,
	capability string, attempt int32, replace bool,
) workstore.Lease {
	t.Helper()
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: sessionID, Mode: workstore.ModeFenced, CandidateOwner: capability,
		WorkerID: workerID, Attempt: attempt, ReplaceOwner: replace,
	})
	if err != nil {
		t.Fatalf("claim generation for %s attempt %d: %v", workerID, attempt, err)
	}
	if decision.Action != workstore.ActionLaunch {
		t.Fatalf("claim action = %q, want launch", decision.Action)
	}
	if err := store.RegisterProcess(context.Background(), decision.Lease, supervisorTestProcess(decision.Lease.Generation)); err != nil {
		t.Fatalf("register generation %d: %v", decision.Lease.Generation, err)
	}
	return decision.Lease
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
