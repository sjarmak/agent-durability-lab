package failureinject

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestArrivalBlocksUntilExactBarrierRelease(t *testing.T) {
	coordinator := NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	client := NewClient(server.URL)

	arrival := Arrival{
		ID: "arrival-1", Point: "before-effect", SessionID: "session-1",
		OwnerTokenHash: "token-hash", Generation: 1, ActorID: "agent-1", PID: 123,
	}
	result := make(chan error, 1)
	go func() {
		result <- client.Arrive(context.Background(), arrival)
	}()

	arrivals, err := coordinator.WaitForArrivals(context.Background(), "before-effect", 1)
	if err != nil {
		t.Fatalf("wait for arrival: %v", err)
	}
	if len(arrivals) != 1 || arrivals[0].ID != arrival.ID {
		t.Fatalf("arrivals = %+v; want %+v", arrivals, arrival)
	}
	select {
	case err := <-result:
		t.Fatalf("arrival returned before release: %v", err)
	default:
	}

	if err := coordinator.Release("before-effect"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("arrival after release: %v", err)
	}
}

func TestDuplicateArrivalIDDoesNotSatisfyTwoActorBarrier(t *testing.T) {
	coordinator := NewCoordinator()
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	client := NewClient(server.URL)
	ctx, cancel := context.WithCancel(context.Background())

	arrival := Arrival{ID: "same", Point: "before-effect", SessionID: "session-1", ActorID: "agent-1"}
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- client.Arrive(ctx, arrival)
		}()
	}
	if _, err := coordinator.WaitForArrivals(context.Background(), "before-effect", 1); err != nil {
		t.Fatalf("wait for first arrival: %v", err)
	}
	if got := coordinator.ArrivalCount("before-effect"); got != 1 {
		t.Fatalf("arrival count = %d; want 1", got)
	}

	cancel()
	for range 2 {
		<-results
	}
}

func TestWaitForArrivalsHonorsCancellation(t *testing.T) {
	coordinator := NewCoordinator()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := coordinator.WaitForArrivals(ctx, "never", 1); err == nil {
		t.Fatal("wait returned nil error after cancellation")
	}
}

func TestUnknownBarrierCannotBeReleased(t *testing.T) {
	coordinator := NewCoordinator()
	if err := coordinator.Release("unknown"); err == nil {
		t.Fatal("release unknown barrier returned nil error")
	}
}

func TestHandlerRejectsTrailingJSON(t *testing.T) {
	coordinator := NewCoordinator()
	body := `{"id":"arrival-1","point":"point","session_id":"session","actor_id":"agent"} {"extra":true}`
	request := httptest.NewRequest(http.MethodPost, "/v1/arrivals", strings.NewReader(body))
	response := httptest.NewRecorder()
	coordinator.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400", response.Code)
	}
}

func TestDuplicateArrivalIdentityConflict(t *testing.T) {
	coordinator := NewCoordinator()
	first := Arrival{ID: "same", Point: "point", SessionID: "session", ActorID: "agent-1"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.arrive(ctx, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled first arrival = %v; want context.Canceled", err)
	}
	conflict := first
	conflict.ActorID = "agent-2"
	if err := coordinator.arrive(context.Background(), conflict); !errors.Is(err, ErrInvalidBarrier) {
		t.Fatalf("conflict = %v; want ErrInvalidBarrier", err)
	}
}

func TestCoordinatorValidationAndIdempotentRelease(t *testing.T) {
	coordinator := NewCoordinator()
	if _, err := coordinator.WaitForArrivals(context.Background(), "", 0); !errors.Is(err, ErrInvalidBarrier) {
		t.Fatalf("invalid wait = %v; want ErrInvalidBarrier", err)
	}
	if got := coordinator.ArrivalCount("missing"); got != 0 {
		t.Fatalf("missing arrival count = %d", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	arrival := Arrival{ID: "arrival", Point: "point", SessionID: "session", ActorID: "agent"}
	if err := coordinator.arrive(ctx, arrival); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled arrival = %v; want context.Canceled", err)
	}
	if err := coordinator.Release("point"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := coordinator.Release("point"); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if err := coordinator.arrive(context.Background(), arrival); err != nil {
		t.Fatalf("same arrival after release: %v", err)
	}
}

func TestClientValidationAndHTTPFailures(t *testing.T) {
	valid := Arrival{ID: "arrival", Point: "point", SessionID: "session", ActorID: "agent"}
	tests := []struct {
		name   string
		client *Client
		value  Arrival
	}{
		{name: "missing URL", client: NewClient(""), value: valid},
		{name: "missing HTTP client", client: NewClientWithHTTP("http://example.invalid", nil), value: valid},
		{name: "invalid arrival", client: NewClient("http://example.invalid"), value: Arrival{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.client.Arrive(context.Background(), test.value); err == nil {
				t.Fatal("Arrive returned nil error")
			}
		})
	}
	statusClient := NewClientWithHTTP("http://barrier.invalid/", &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader("conflict"))}, nil
	})})
	if err := statusClient.Arrive(context.Background(), valid); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("status error = %v", err)
	}
	transportClient := NewClientWithHTTP("http://barrier.invalid", &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport failed")
	})})
	if err := transportClient.Arrive(context.Background(), valid); err == nil || !strings.Contains(err.Error(), "transport failed") {
		t.Fatalf("transport error = %v", err)
	}
}

func TestHandlerRejectsMalformedUnknownAndIncompleteArrivals(t *testing.T) {
	coordinator := NewCoordinator()
	for _, body := range [][]byte{
		[]byte(`not-json`),
		[]byte(`{"id":"one","point":"point","session_id":"session","actor_id":"agent","unknown":true}`),
		[]byte(`{"id":"one"}`),
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/arrivals", bytes.NewReader(body))
		response := httptest.NewRecorder()
		coordinator.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("body %q status = %d; want 400", body, response.Code)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
