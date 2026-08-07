package lab

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxDestinationRequestBytes = 64 << 10

type HTTPDestination struct {
	listener net.Listener
	server   *http.Server
	mu       sync.Mutex
	effects  map[Destination][]PhysicalEffect
	byKey    map[Destination]map[string]PhysicalEffect
	sequence int
}

func StartHTTPDestination() (*HTTPDestination, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for HTTP destination: %w", err)
	}
	destination := &HTTPDestination{
		listener: listener,
		effects:  make(map[Destination][]PhysicalEffect),
		byKey:    make(map[Destination]map[string]PhysicalEffect),
	}
	destination.server = &http.Server{
		Handler:           destination.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = destination.server.Serve(listener) }()
	return destination, nil
}

func (d *HTTPDestination) URL() string {
	return "http://" + d.listener.Addr().String()
}

func (d *HTTPDestination) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.server.Shutdown(ctx)
}

func (d *HTTPDestination) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/idempotent-api", d.handleMutation(DestinationIdempotentAPI))
	mux.HandleFunc("POST /v1/non-idempotent-api", d.handleMutation(DestinationNonIdempotentAPI))
	mux.HandleFunc("GET /v1/non-idempotent-api/reconcile", d.handleReconcile)
	mux.HandleFunc("POST /v1/messages", d.handleMutation(DestinationMessage))
	mux.HandleFunc("GET /v1/state/{kind}", d.handleState)
	return mux
}

func (d *HTTPDestination) handleMutation(kind Destination) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		var effectRequest EffectRequest
		if err := decodeHTTPJSON(response, request, &effectRequest); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		if err := validateEffectRequest(kind, effectRequest); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		key := ""
		switch kind {
		case DestinationIdempotentAPI:
			key = request.Header.Get("Idempotency-Key")
		case DestinationMessage:
			key = request.Header.Get("Message-ID")
		}
		result, err := d.mutate(kind, effectRequest, key)
		if err != nil {
			http.Error(response, err.Error(), http.StatusConflict)
			return
		}
		writeHTTPJSON(response, result)
	}
}

func (d *HTTPDestination) mutate(kind Destination, request EffectRequest, key string) (EffectResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if key != "" {
		if existing, found := d.byKey[kind][key]; found {
			if existing.LogicalID != request.EffectID || existing.Payload != request.Payload {
				return EffectResult{}, fmt.Errorf("key %q was reused with conflicting effect content", key)
			}
			return EffectResult{Receipt: existing.Receipt, Outcome: OutcomeDeduplicated}, nil
		}
	}
	d.sequence++
	physicalID := string(kind) + "-" + strconv.Itoa(d.sequence)
	effect := PhysicalEffect{
		PhysicalID: physicalID, LogicalID: request.EffectID, Receipt: string(kind) + ":" + physicalID,
		Payload: request.Payload, AppliedAt: time.Now().UTC(), Attempt: request.Attempt, Kind: kind,
	}
	d.effects[kind] = append(d.effects[kind], effect)
	if key != "" {
		if d.byKey[kind] == nil {
			d.byKey[kind] = make(map[string]PhysicalEffect)
		}
		d.byKey[kind][key] = effect
	}
	return EffectResult{Receipt: effect.Receipt, Outcome: OutcomeApplied}, nil
}

func (d *HTTPDestination) handleReconcile(response http.ResponseWriter, request *http.Request) {
	effectID := request.URL.Query().Get("effect_id")
	payloadHash := request.URL.Query().Get("payload_hash")
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, effect := range d.effects[DestinationNonIdempotentAPI] {
		if effect.LogicalID == effectID {
			if payloadHash == "" || payloadHash != hashPayload(effect.Payload) {
				http.Error(response, "correlation ID has conflicting effect content", http.StatusConflict)
				return
			}
			writeHTTPJSON(response, EffectResult{Receipt: effect.Receipt, Outcome: OutcomeReconciled})
			return
		}
	}
	http.Error(response, "effect not found", http.StatusNotFound)
}

func (d *HTTPDestination) handleState(response http.ResponseWriter, request *http.Request) {
	kind := Destination(request.PathValue("kind"))
	if !kind.Valid() {
		http.Error(response, "invalid destination", http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	effects := append([]PhysicalEffect(nil), d.effects[kind]...)
	d.mu.Unlock()
	writeHTTPJSON(response, DestinationState{PhysicalEffects: effects})
}

func applyHTTPEffect(
	ctx context.Context,
	destination Destination,
	baseURL string,
	request EffectRequest,
) (EffectResult, error) {
	if destination == DestinationNonIdempotentAPI && request.Mode == ModeProtected {
		reconciled, found, err := reconcileHTTP(ctx, baseURL, request.EffectID, request.Payload)
		if err != nil {
			return EffectResult{}, err
		}
		if found {
			return reconciled, nil
		}
	}
	path := "/v1/" + string(destination)
	if destination == DestinationMessage {
		path = "/v1/messages"
	}
	body, err := json.Marshal(request)
	if err != nil {
		return EffectResult{}, fmt.Errorf("encode HTTP effect: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return EffectResult{}, fmt.Errorf("create HTTP effect request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if request.Mode == ModeProtected {
		if destination == DestinationIdempotentAPI {
			httpRequest.Header.Set("Idempotency-Key", request.EffectID)
		}
		if destination == DestinationMessage {
			httpRequest.Header.Set("Message-ID", request.EffectID)
		}
	}
	return executeEffectRequest(httpRequest)
}

func reconcileHTTP(ctx context.Context, baseURL, effectID, payload string) (EffectResult, bool, error) {
	values := url.Values{"effect_id": {effectID}, "payload_hash": {hashPayload(payload)}}
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/non-idempotent-api/reconcile?" + values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return EffectResult{}, false, fmt.Errorf("create reconciliation request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return EffectResult{}, false, fmt.Errorf("query non-idempotent API: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return EffectResult{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return EffectResult{}, false, readHTTPError("query non-idempotent API", response)
	}
	var result EffectResult
	if err := json.NewDecoder(io.LimitReader(response.Body, maxDestinationRequestBytes)).Decode(&result); err != nil {
		return EffectResult{}, false, fmt.Errorf("decode reconciliation response: %w", err)
	}
	return result, true, nil
}

func hashPayload(payload string) string {
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func executeEffectRequest(request *http.Request) (EffectResult, error) {
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return EffectResult{}, fmt.Errorf("perform HTTP effect: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return EffectResult{}, readHTTPError("perform HTTP effect", response)
	}
	var result EffectResult
	if err := json.NewDecoder(io.LimitReader(response.Body, maxDestinationRequestBytes)).Decode(&result); err != nil {
		return EffectResult{}, fmt.Errorf("decode HTTP effect response: %w", err)
	}
	return result, nil
}

func snapshotHTTPDestination(ctx context.Context, baseURL string, destination Destination) (DestinationState, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/v1/state/"+string(destination), nil)
	if err != nil {
		return DestinationState{}, fmt.Errorf("create HTTP snapshot request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return DestinationState{}, fmt.Errorf("read HTTP destination state: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return DestinationState{}, readHTTPError("read HTTP destination state", response)
	}
	var state DestinationState
	if err := json.NewDecoder(io.LimitReader(response.Body, maxDestinationRequestBytes)).Decode(&state); err != nil {
		return DestinationState{}, fmt.Errorf("decode HTTP destination state: %w", err)
	}
	return state, nil
}

func decodeHTTPJSON(response http.ResponseWriter, request *http.Request, value any) error {
	request.Body = http.MaxBytesReader(response, request.Body, maxDestinationRequestBytes)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("decode JSON: trailing data")
	}
	return nil
}

func writeHTTPJSON(response http.ResponseWriter, value any) {
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(value); err != nil {
		return
	}
}

func readHTTPError(operation string, response *http.Response) error {
	message, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if err != nil {
		return fmt.Errorf("%s: status %d; read response: %w", operation, response.StatusCode, err)
	}
	return fmt.Errorf("%s: status %d: %s", operation, response.StatusCode, strings.TrimSpace(string(message)))
}
