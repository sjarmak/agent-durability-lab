package failureinject

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthenticatedCoordinatorAcceptsOnlyPreregisteredIdentity(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatalf("new credential: %v", err)
	}
	expected := Expectation{Point: "before-effect", SessionID: "session-1", Generation: 2, ActorID: "agent-2"}
	coordinator, err := NewAuthenticatedCoordinator(credential, expected)
	if err != nil {
		t.Fatalf("new authenticated coordinator: %v", err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	arrival := Arrival{
		ID: "arrival-1", Point: expected.Point, SessionID: expected.SessionID,
		Generation: expected.Generation, ActorID: expected.ActorID,
	}

	result := make(chan error, 1)
	go func() { result <- NewAuthenticatedClient(server.URL, credential).Arrive(context.Background(), arrival) }()
	arrivals, err := coordinator.WaitForArrivals(context.Background(), expected.Point, 1)
	if err != nil {
		t.Fatalf("wait for authenticated arrival: %v", err)
	}
	if len(arrivals) != 1 || !sameArrival(arrivals[0], arrival) {
		t.Fatalf("arrivals = %+v; want %+v", arrivals, arrival)
	}
	if err := coordinator.Release(expected.Point); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("authenticated arrival: %v", err)
	}
}

func TestAuthenticatedCoordinatorRejectsForgeryIdentitySubstitutionAndReplay(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatalf("new credential: %v", err)
	}
	expected := Expectation{Point: "before-effect", SessionID: "session-1", Generation: 1, ActorID: "agent-1"}
	coordinator, err := NewAuthenticatedCoordinator(credential, expected)
	if err != nil {
		t.Fatalf("new authenticated coordinator: %v", err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	valid := Arrival{ID: "arrival-1", Point: expected.Point, SessionID: expected.SessionID, Generation: 1, ActorID: expected.ActorID}

	wrongCredential, err := NewCredential()
	if err != nil {
		t.Fatalf("new wrong credential: %v", err)
	}
	if err := NewAuthenticatedClient(server.URL, wrongCredential).Arrive(context.Background(), valid); err == nil {
		t.Fatal("forged credential was accepted")
	}
	changed := valid
	changed.ActorID = "attacker"
	if err := NewAuthenticatedClient(server.URL, credential).Arrive(context.Background(), changed); err == nil {
		t.Fatal("unregistered actor was accepted")
	}
	if got := coordinator.ArrivalCount(expected.Point); got != 0 {
		t.Fatalf("rejected arrivals changed count to %d", got)
	}

	request, err := authenticatedRequest(context.Background(), server.URL, credential, valid)
	if err != nil {
		t.Fatalf("create authenticated request: %v", err)
	}
	coordinator.mu.Lock()
	state := coordinator.ensurePointLocked(expected.Point)
	state.isReleased = true
	close(state.released)
	coordinator.mu.Unlock()
	for attempt := 1; attempt <= 2; attempt++ {
		clone := request.Clone(context.Background())
		clone.Body, err = request.GetBody()
		if err != nil {
			t.Fatalf("clone request body: %v", err)
		}
		response, requestErr := http.DefaultClient.Do(clone)
		if requestErr != nil {
			t.Fatalf("request %d: %v", attempt, requestErr)
		}
		_ = response.Body.Close()
		want := http.StatusNoContent
		if attempt == 2 {
			want = http.StatusForbidden
		}
		if response.StatusCode != want {
			t.Fatalf("request %d status = %d; want %d", attempt, response.StatusCode, want)
		}
	}
	if got := coordinator.ArrivalCount(expected.Point); got != 1 {
		t.Fatalf("replay changed count to %d", got)
	}
}

func TestRejectedArrivalIDReuseDoesNotConsumeAnotherExpectation(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatal(err)
	}
	first := Expectation{Point: "point", SessionID: "session-1", Generation: 1, ActorID: "actor-1"}
	second := Expectation{Point: "point", SessionID: "session-2", Generation: 1, ActorID: "actor-2"}
	coordinator, err := NewAuthenticatedCoordinator(credential, first, second)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	coordinator.mu.Lock()
	state := coordinator.ensurePointLocked(first.Point)
	state.isReleased = true
	close(state.released)
	coordinator.mu.Unlock()

	client := NewAuthenticatedClient(server.URL, credential)
	if err := client.Arrive(context.Background(), Arrival{
		ID: "shared", Point: first.Point, SessionID: first.SessionID, Generation: 1, ActorID: first.ActorID,
	}); err != nil {
		t.Fatalf("first arrival: %v", err)
	}
	reused := Arrival{ID: "shared", Point: second.Point, SessionID: second.SessionID, Generation: 1, ActorID: second.ActorID}
	if err := client.Arrive(context.Background(), reused); err == nil {
		t.Fatal("reused arrival ID was accepted")
	}
	reused.ID = "second"
	if err := client.Arrive(context.Background(), reused); err != nil {
		t.Fatalf("rejected reuse consumed expectation: %v", err)
	}
	if got := coordinator.ArrivalCount(first.Point); got != 2 {
		t.Fatalf("arrival count = %d; want 2", got)
	}
}

func TestAuthenticatedArrivalIDCannotReplayAcrossPoints(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatal(err)
	}
	first := Expectation{Point: "point-1", SessionID: "session", Generation: 1, ActorID: "actor"}
	second := Expectation{Point: "point-2", SessionID: "session", Generation: 1, ActorID: "actor"}
	coordinator, err := NewAuthenticatedCoordinator(credential, first, second)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	for _, point := range []string{first.Point, second.Point} {
		coordinator.mu.Lock()
		state := coordinator.ensurePointLocked(point)
		state.isReleased = true
		close(state.released)
		coordinator.mu.Unlock()
	}
	client := NewAuthenticatedClient(server.URL, credential)
	arrival := Arrival{ID: "global-arrival", Point: first.Point, SessionID: first.SessionID, Generation: 1, ActorID: first.ActorID}
	if err := client.Arrive(context.Background(), arrival); err != nil {
		t.Fatalf("first point: %v", err)
	}
	arrival.Point = second.Point
	if err := client.Arrive(context.Background(), arrival); err == nil {
		t.Fatal("arrival ID replay across points was accepted")
	}
	arrival.ID = "second-arrival"
	if err := client.Arrive(context.Background(), arrival); err != nil {
		t.Fatalf("replay rejection consumed second expectation: %v", err)
	}
	if coordinator.ArrivalCount(first.Point) != 1 || coordinator.ArrivalCount(second.Point) != 1 {
		t.Fatalf("arrival counts = %d/%d; want 1/1", coordinator.ArrivalCount(first.Point), coordinator.ArrivalCount(second.Point))
	}
}

func TestAuthenticatedExpectationIsConsumedExactlyOnceUnderConcurrency(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatal(err)
	}
	expected := Expectation{Point: "point", SessionID: "session", Generation: 1, ActorID: "actor"}
	coordinator, err := NewAuthenticatedCoordinator(credential, expected)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	coordinator.mu.Lock()
	state := coordinator.ensurePointLocked(expected.Point)
	state.isReleased = true
	close(state.released)
	coordinator.mu.Unlock()

	const contenders = 32
	results := make(chan error, contenders)
	for index := range contenders {
		arrival := Arrival{
			ID: fmt.Sprintf("arrival-%d", index), Point: expected.Point,
			SessionID: expected.SessionID, Generation: expected.Generation, ActorID: expected.ActorID,
		}
		go func() {
			results <- NewAuthenticatedClient(server.URL, credential).Arrive(context.Background(), arrival)
		}()
	}
	accepted := 0
	for range contenders {
		if err := <-results; err == nil {
			accepted++
		}
	}
	if accepted != 1 || coordinator.ArrivalCount(expected.Point) != 1 {
		t.Fatalf("accepted = %d, arrivals = %d; want exactly one", accepted, coordinator.ArrivalCount(expected.Point))
	}
}

func TestAuthenticatedCoordinatorRejectsDuplicateAuthenticationHeaders(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatal(err)
	}
	expected := Expectation{Point: "point", SessionID: "session", Generation: 1, ActorID: "actor"}
	coordinator, err := NewAuthenticatedCoordinator(credential, expected)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(coordinator.Handler())
	t.Cleanup(server.Close)
	arrival := Arrival{ID: "arrival", Point: expected.Point, SessionID: expected.SessionID, Generation: 1, ActorID: expected.ActorID}
	coordinator.mu.Lock()
	state := coordinator.ensurePointLocked(expected.Point)
	state.isReleased = true
	close(state.released)
	coordinator.mu.Unlock()

	for _, header := range []string{authorizationHeader, nonceHeader} {
		request, requestErr := authenticatedRequest(context.Background(), server.URL, credential, arrival)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		request.Header.Add(header, "attacker-controlled-duplicate")
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("duplicate %s status = %d; want 401", header, response.StatusCode)
		}
	}
	if coordinator.ArrivalCount(expected.Point) != 0 {
		t.Fatal("duplicate headers mutated barrier state")
	}
}

func TestCredentialIsOpaqueAndEnvironmentRoundTrips(t *testing.T) {
	credential, err := NewCredential()
	if err != nil {
		t.Fatalf("new credential: %v", err)
	}
	var raw bytes.Buffer
	if err := credential.Write(&raw); err != nil {
		t.Fatalf("write credential: %v", err)
	}
	if rendered := fmt.Sprintf("%v", credential); bytes.Contains([]byte(rendered), raw.Bytes()) || rendered != "[REDACTED]" {
		t.Fatalf("credential formatting = %q", rendered)
	}
	if encoded, marshalErr := json.Marshal(credential); marshalErr == nil || bytes.Contains(encoded, raw.Bytes()) {
		t.Fatalf("credential JSON = %q, error = %v", encoded, marshalErr)
	}
	decoded, err := readCredential(bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatalf("parse credential: %v", err)
	}
	var decodedRaw bytes.Buffer
	if err := decoded.Write(&decodedRaw); err != nil {
		t.Fatalf("write parsed credential: %v", err)
	}
	if !bytes.Equal(decodedRaw.Bytes(), raw.Bytes()) {
		t.Fatal("credential did not round trip")
	}
}

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
