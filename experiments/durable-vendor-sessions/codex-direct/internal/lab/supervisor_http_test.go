package lab

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

func TestSupervisorHTTPAcceptsCurrentEffectAndRejectsStaleCapability(t *testing.T) {
	store := openCodexSupervisorStore(t)
	decision, err := store.StartOrAttach(context.Background(), workstore.StartRequest{
		SessionID: "session-1", Mode: workstore.ModeFenced, CandidateOwner: "owner-1",
		WorkerID: "worker-1", Attempt: 1,
	})
	if err != nil {
		t.Fatalf("claim authority: %v", err)
	}
	if err := store.RegisterProcess(context.Background(), decision.Lease, codexSupervisorTestProcess(1)); err != nil {
		t.Fatalf("register process: %v", err)
	}
	supervisor := newTurnSupervisor(context.Background(), store,
		func(context.Context, *workstore.Store, workstore.Lease) (supervisedResult, error) {
			return supervisedResult{}, errors.New("unused")
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	client := newSupervisorClient(server.URL, nil)
	request := supervisorEffectRequest{
		SessionID: "session-1", Generation: 1, OwnerCapability: "owner-1",
		EffectID: "effect-1", Value: "controlled-edit",
	}
	if err := client.CommitEffect(context.Background(), request); err != nil {
		t.Fatalf("commit current effect: %v", err)
	}
	request.Generation = 2
	request.OwnerCapability = "stale"
	if err := client.CommitEffect(context.Background(), request); !errors.Is(err, workstore.ErrStaleOwner) {
		t.Fatalf("stale effect = %v", err)
	}
	snapshot, err := store.Snapshot(context.Background(), "session-1")
	if err != nil || len(snapshot.Effects) != 1 || snapshot.Effects[0].Generation != 1 {
		t.Fatalf("snapshot = %+v err=%v", snapshot, err)
	}
}

func TestSupervisorWireErrorsPreserveDomainSemantics(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
		want error
	}{
		{name: "stale", err: workstore.ErrStaleOwner, code: "stale_owner", want: workstore.ErrStaleOwner},
		{name: "canceled", err: workstore.ErrSessionCanceled, code: "session_canceled", want: workstore.ErrSessionCanceled},
		{name: "missing", err: workstore.ErrSessionNotFound, code: "session_not_found", want: workstore.ErrSessionNotFound},
		{name: "invalid", err: workstore.ErrInvalidRequest, code: "invalid_request", want: workstore.ErrInvalidRequest},
		{name: "deadline", err: context.DeadlineExceeded, code: "request_ended", want: context.Canceled},
		{name: "unavailable", err: errSupervisorExecutionUnavailable, code: "execution_unavailable", want: errSupervisorExecutionUnavailable},
		{name: "unknown", err: errors.New("unexpected"), code: "unknown", want: errors.New("supervisor request failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeSupervisorDomainError(response, test.err)
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("cache control = %q", response.Header().Get("Cache-Control"))
			}
			mapped := mapSupervisorWireError(test.code)
			if test.name == "unknown" {
				if mapped == nil || mapped.Error() != test.want.Error() {
					t.Fatalf("mapped error = %v", mapped)
				}
			} else if !errors.Is(mapped, test.want) {
				t.Fatalf("mapped error = %v, want %v", mapped, test.want)
			}
		})
	}
}

func TestSupervisorClientRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "invalid-error-body", status: http.StatusConflict, body: "not-json"},
		{name: "unknown-error", status: http.StatusConflict, body: `{"code":"unknown"}`},
		{name: "unknown-success-field", status: http.StatusOK, body: `{"generation":1,"unknown":true}`},
		{name: "trailing-success", status: http.StatusOK, body: `{"generation":1} {}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newSupervisorClient("http://127.0.0.1:8080", &http.Client{
				Timeout: time.Second,
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: test.status, Header: make(http.Header),
						Body: io.NopCloser(bytes.NewBufferString(test.body)),
					}, nil
				}),
			})
			var response supervisorReceipt
			if err := client.post(context.Background(), supervisorStartPath,
				supervisorStartRequest{SessionID: "session-1", WorkerID: "worker-1", Attempt: 1},
				&response); err == nil {
				t.Fatal("malformed supervisor response was accepted")
			}
		})
	}
}

func TestSupervisorClientRejectsIncompleteAuthorityReceipts(t *testing.T) {
	clientFor := func(body string) *supervisorClient {
		return newSupervisorClient("http://127.0.0.1:8080", &http.Client{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Header: make(http.Header),
					Body: io.NopCloser(bytes.NewBufferString(body)),
				}, nil
			}),
		})
	}
	if _, err := clientFor(`{"generation":1}`).StartOrAttach(context.Background(), supervisorStartRequest{
		SessionID: "session-1", WorkerID: "worker-1", Attempt: 1,
	}); err == nil {
		t.Fatal("incomplete start-or-attach receipt was accepted")
	}
	if err := clientFor(`{"accepted":true,"generation":1,"owner_token_hash":"wrong"}`).CommitEffect(
		context.Background(), supervisorEffectRequest{
			SessionID: "session-1", Generation: 1, OwnerCapability: "owner-1",
			EffectID: "effect-1", Value: "controlled-edit",
		}); err == nil {
		t.Fatal("mismatched effect authority receipt was accepted")
	}
	transportErr := errors.New("transport failed")
	client := newSupervisorClient("http://127.0.0.1:8080", &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, transportErr }),
	})
	if _, err := client.Cancel(context.Background(), supervisorCancelRequest{
		SessionID: "session-1", RequestID: "cancel-1",
	}); !errors.Is(err, transportErr) {
		t.Fatalf("transport failure = %v", err)
	}
}

func TestSupervisorClientRejectsNonLoopbackCapabilityTransport(t *testing.T) {
	transported := atomic.Bool{}
	client := newSupervisorClient("https://example.com", &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		transported.Store(true)
		return nil, errors.New("must not transport")
	})})
	err := client.CommitEffect(context.Background(), supervisorEffectRequest{
		SessionID: "session-1", Generation: 1, OwnerCapability: "owner-secret",
		EffectID: "effect-1", Value: "controlled-edit",
	})
	if !errors.Is(err, workstore.ErrInvalidRequest) || transported.Load() {
		t.Fatalf("non-loopback request error=%v transported=%t", err, transported.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestFencedActivityDelegatesToSupervisorService(t *testing.T) {
	store := openCodexSupervisorStore(t)
	supervisor := newTurnSupervisor(context.Background(), store,
		func(ctx context.Context, store *workstore.Store, lease workstore.Lease) (supervisedResult, error) {
			if err := store.RegisterProcess(ctx, lease, codexSupervisorTestProcess(lease.Generation)); err != nil {
				return supervisedResult{}, err
			}
			if err := store.CommitEffectOnce(ctx, lease, workstore.Effect{ID: "effect-1", Value: "controlled-edit"}); err != nil {
				return supervisedResult{}, err
			}
			return supervisedResult{
				ThreadID: testThreadID, PhysicalAttemptID: "supervisor-generation-1",
				ProcessIdentity: "pid:101:start:boot:1", Outcome: workstore.Outcome{Value: "EFFECT_COMPLETE"},
			}, nil
		}, sequentialCapabilities())
	server := httptest.NewServer(newSupervisorHandler(supervisor))
	t.Cleanup(server.Close)
	activities := testActivities(t.TempDir())
	activities.SupervisorURL = server.URL
	result, err := activities.runFencedCodex(context.Background(), CodexActivityInput{
		LogicalSessionID: "session-1", LogicalTurnID: "turn-1", LogicalEffectID: "effect-1",
		RecoveryMode: RecoveryModeFenced,
	}, 1)
	if err != nil {
		t.Fatalf("run fenced Activity: %v", err)
	}
	if result.ThreadID != testThreadID || result.PhysicalAttemptID != "supervisor-generation-1" ||
		result.ProcessIdentity != "pid:101:start:boot:1" || result.Result != "EFFECT_COMPLETE" {
		t.Fatalf("result = %+v", result)
	}
}
