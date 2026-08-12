package failureinject

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	ErrInvalidBarrier  = errors.New("invalid barrier request")
	ErrBarrierNotFound = errors.New("barrier not found")
)

type Arrival struct {
	ID             string    `json:"id"`
	Point          string    `json:"point"`
	SessionID      string    `json:"session_id"`
	OwnerTokenHash string    `json:"owner_token_hash,omitempty"`
	Generation     uint64    `json:"generation,omitempty"`
	ActorID        string    `json:"actor_id"`
	PID            int       `json:"pid,omitempty"`
	ProcessStart   string    `json:"process_start,omitempty"`
	Time           time.Time `json:"time"`
}

type Coordinator struct {
	mu             sync.Mutex
	points         map[string]*pointState
	authentication *authentication
}

type pointState struct {
	arrivals   []Arrival
	byID       map[string]Arrival
	changed    chan struct{}
	released   chan struct{}
	isReleased bool
}

func NewCoordinator() *Coordinator {
	return &Coordinator{points: make(map[string]*pointState)}
}

func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/arrivals", c.handleArrival)
	return mux
}

func (c *Coordinator) WaitForArrivals(ctx context.Context, point string, count int) ([]Arrival, error) {
	if point == "" || count < 1 {
		return nil, fmt.Errorf("%w: point and positive count are required", ErrInvalidBarrier)
	}
	for {
		c.mu.Lock()
		state := c.ensurePointLocked(point)
		if len(state.arrivals) >= count {
			arrivals := append([]Arrival(nil), state.arrivals...)
			c.mu.Unlock()
			return arrivals, nil
		}
		changed := state.changed
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (c *Coordinator) ArrivalCount(point string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.points[point]
	if !found {
		return 0
	}
	return len(state.arrivals)
}

func (c *Coordinator) Release(point string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, found := c.points[point]
	if !found {
		return fmt.Errorf("%w: %s", ErrBarrierNotFound, point)
	}
	if state.isReleased {
		return nil
	}
	state.isReleased = true
	close(state.released)
	return nil
}

func (c *Coordinator) handleArrival(response http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(response, request.Body, 64<<10)
	defer func() { _ = request.Body.Close() }()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(response, "invalid arrival", http.StatusBadRequest)
		return
	}
	var nonce string
	if c.authentication != nil {
		nonce, err = c.authentication.verify(request, body)
		if err != nil {
			http.Error(response, "unauthorized arrival", http.StatusUnauthorized)
			return
		}
	}

	var arrival Arrival
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&arrival); err != nil {
		http.Error(response, "invalid arrival", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		http.Error(response, "invalid arrival", http.StatusBadRequest)
		return
	}
	if err := validateArrival(arrival); err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.authorizeAndArrive(request.Context(), nonce, arrival); err != nil {
		if errors.Is(err, ErrUnauthorizedBarrier) {
			http.Error(response, err.Error(), http.StatusForbidden)
			return
		}
		if errors.Is(err, ErrInvalidBarrier) {
			http.Error(response, err.Error(), http.StatusConflict)
			return
		}
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (c *Coordinator) authorizeAndArrive(ctx context.Context, nonce string, arrival Arrival) error {
	if c.authentication == nil {
		return c.arrive(ctx, arrival)
	}
	expectation := Expectation{
		Point: arrival.Point, SessionID: arrival.SessionID,
		Generation: arrival.Generation, ActorID: arrival.ActorID,
	}
	c.mu.Lock()
	if _, registered := c.authentication.expected[expectation]; !registered {
		c.mu.Unlock()
		return fmt.Errorf("%w: arrival identity was not preregistered", ErrUnauthorizedBarrier)
	}
	if _, replayed := c.authentication.usedNonces[nonce]; replayed {
		c.mu.Unlock()
		return fmt.Errorf("%w: arrival nonce was replayed", ErrInvalidBarrier)
	}
	if _, reused := c.authentication.usedArrivalIDs[arrival.ID]; reused {
		c.mu.Unlock()
		return fmt.Errorf("%w: arrival ID %q was replayed", ErrInvalidBarrier, arrival.ID)
	}
	state := c.ensurePointLocked(arrival.Point)
	delete(c.authentication.expected, expectation)
	c.authentication.usedNonces[nonce] = struct{}{}
	c.authentication.usedArrivalIDs[arrival.ID] = struct{}{}
	released := c.recordArrivalLocked(state, arrival)
	c.mu.Unlock()
	return waitForRelease(ctx, released)
}

func (c *Coordinator) arrive(ctx context.Context, arrival Arrival) error {
	c.mu.Lock()
	state := c.ensurePointLocked(arrival.Point)
	if existing, found := state.byID[arrival.ID]; found {
		if c.authentication != nil {
			c.mu.Unlock()
			return fmt.Errorf("%w: arrival ID %q was replayed", ErrInvalidBarrier, arrival.ID)
		}
		if !sameArrival(existing, arrival) {
			c.mu.Unlock()
			return fmt.Errorf("%w: arrival ID %q was reused with different identity", ErrInvalidBarrier, arrival.ID)
		}
		released := state.released
		c.mu.Unlock()
		return waitForRelease(ctx, released)
	}
	released := c.recordArrivalLocked(state, arrival)
	c.mu.Unlock()
	return waitForRelease(ctx, released)
}

func (c *Coordinator) recordArrivalLocked(state *pointState, arrival Arrival) <-chan struct{} {
	arrival.Time = time.Now().UTC()
	state.byID[arrival.ID] = arrival
	state.arrivals = append(state.arrivals, arrival)
	close(state.changed)
	state.changed = make(chan struct{})
	return state.released
}

func (c *Coordinator) ensurePointLocked(point string) *pointState {
	state, found := c.points[point]
	if found {
		return state
	}
	state = &pointState{
		byID: make(map[string]Arrival), changed: make(chan struct{}), released: make(chan struct{}),
	}
	c.points[point] = state
	return state
}

func validateArrival(arrival Arrival) error {
	if arrival.ID == "" || arrival.Point == "" || arrival.SessionID == "" || arrival.ActorID == "" {
		return fmt.Errorf("%w: ID, point, session, and actor are required", ErrInvalidBarrier)
	}
	return nil
}

func sameArrival(left, right Arrival) bool {
	return left.ID == right.ID && left.Point == right.Point && left.SessionID == right.SessionID &&
		left.OwnerTokenHash == right.OwnerTokenHash && left.Generation == right.Generation &&
		left.ActorID == right.ActorID && left.PID == right.PID && left.ProcessStart == right.ProcessStart
}

func waitForRelease(ctx context.Context, released <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-released:
		return nil
	}
}
