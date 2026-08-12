package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const (
	supervisorStartPath  = "/v1/turns/start-or-attach"
	supervisorCancelPath = "/v1/turns/cancel"
	supervisorEffectPath = "/v1/effects/commit"
	supervisorMaxBytes   = 64 << 10
)

type supervisorCancelRequest struct {
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

type supervisorEffectRequest struct {
	SessionID       string `json:"session_id"`
	Generation      uint64 `json:"generation"`
	OwnerCapability string `json:"owner_capability"`
	EffectID        string `json:"effect_id"`
	Value           string `json:"value"`
}

type supervisorEffectReceipt struct {
	Accepted       bool   `json:"accepted"`
	Generation     uint64 `json:"generation"`
	OwnerTokenHash string `json:"owner_token_hash"`
}

type supervisorWireError struct {
	Code string `json:"code"`
}

func newSupervisorHandler(supervisor *turnSupervisor) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+supervisorStartPath, func(response http.ResponseWriter, request *http.Request) {
		var start supervisorStartRequest
		if !decodeSupervisorRequest(response, request, &start) || !validSupervisorStart(start) {
			writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
			return
		}
		receipt, err := supervisor.StartOrAttach(request.Context(), start)
		if err != nil {
			writeSupervisorDomainError(response, err)
			return
		}
		writeSupervisorJSON(response, http.StatusOK, receipt)
	})
	mux.HandleFunc("POST "+supervisorCancelPath, func(response http.ResponseWriter, request *http.Request) {
		var cancel supervisorCancelRequest
		if !decodeSupervisorRequest(response, request, &cancel) || cancel.SessionID == "" || cancel.RequestID == "" {
			writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
			return
		}
		decision, err := supervisor.Cancel(request.Context(), cancel.SessionID, cancel.RequestID)
		if err != nil {
			writeSupervisorDomainError(response, err)
			return
		}
		writeSupervisorJSON(response, http.StatusOK, decision)
	})
	mux.HandleFunc("POST "+supervisorEffectPath, func(response http.ResponseWriter, request *http.Request) {
		var effect supervisorEffectRequest
		if !decodeSupervisorRequest(response, request, &effect) || !validSupervisorEffect(effect) {
			writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
			return
		}
		lease := workstore.Lease{
			SessionID: effect.SessionID, Generation: effect.Generation, OwnerToken: effect.OwnerCapability,
		}
		if err := supervisor.store.CommitEffectOnce(request.Context(), lease,
			workstore.Effect{ID: effect.EffectID, Value: effect.Value}); err != nil {
			writeSupervisorDomainError(response, err)
			return
		}
		writeSupervisorJSON(response, http.StatusOK, supervisorEffectReceipt{
			Accepted: true, Generation: effect.Generation,
			OwnerTokenHash: workstore.HashToken(effect.OwnerCapability),
		})
	})
	return mux
}

func decodeSupervisorRequest(response http.ResponseWriter, request *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return false
	}
	request.Body = http.MaxBytesReader(response, request.Body, supervisorMaxBytes)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

func validSupervisorStart(request supervisorStartRequest) bool {
	return request.SessionID != "" && request.WorkerID != "" && request.Attempt > 0
}

func validSupervisorEffect(request supervisorEffectRequest) bool {
	return request.SessionID != "" && request.Generation > 0 && request.OwnerCapability != "" &&
		request.EffectID != "" && request.Value != ""
}

func writeSupervisorDomainError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workstore.ErrStaleOwner):
		writeSupervisorError(response, http.StatusConflict, "stale_owner")
	case errors.Is(err, workstore.ErrSessionCanceled):
		writeSupervisorError(response, http.StatusConflict, "session_canceled")
	case errors.Is(err, workstore.ErrSessionNotFound):
		writeSupervisorError(response, http.StatusNotFound, "session_not_found")
	case errors.Is(err, workstore.ErrInvalidRequest):
		writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeSupervisorError(response, http.StatusRequestTimeout, "request_ended")
	case errors.Is(err, errSupervisorExecutionUnavailable):
		writeSupervisorError(response, http.StatusServiceUnavailable, "execution_unavailable")
	default:
		writeSupervisorError(response, http.StatusInternalServerError, "internal_error")
	}
}

func writeSupervisorError(response http.ResponseWriter, status int, code string) {
	writeSupervisorJSON(response, status, supervisorWireError{Code: code})
}

func writeSupervisorJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

type supervisorClient struct {
	baseURL string
	http    *http.Client
}

func newSupervisorClient(baseURL string, httpClient *http.Client) *supervisorClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &supervisorClient{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

func (c *supervisorClient) StartOrAttach(ctx context.Context, request supervisorStartRequest) (supervisorReceipt, error) {
	var receipt supervisorReceipt
	if err := c.post(ctx, supervisorStartPath, request, &receipt); err != nil {
		return supervisorReceipt{}, err
	}
	if receipt.Generation == 0 || receipt.OwnerTokenHash == "" || receipt.Outcome.Value == "" {
		return supervisorReceipt{}, errors.New("supervisor start receipt is incomplete")
	}
	return receipt, nil
}

func (c *supervisorClient) Cancel(ctx context.Context, request supervisorCancelRequest) (workstore.CancelDecision, error) {
	var decision workstore.CancelDecision
	if err := c.post(ctx, supervisorCancelPath, request, &decision); err != nil {
		return workstore.CancelDecision{}, err
	}
	return decision, nil
}

func (c *supervisorClient) CommitEffect(ctx context.Context, request supervisorEffectRequest) error {
	var receipt supervisorEffectReceipt
	if err := c.post(ctx, supervisorEffectPath, request, &receipt); err != nil {
		return err
	}
	if !receipt.Accepted || receipt.Generation != request.Generation ||
		receipt.OwnerTokenHash != workstore.HashToken(request.OwnerCapability) {
		return errors.New("supervisor effect receipt does not match authority")
	}
	return nil
}

func (c *supervisorClient) post(ctx context.Context, path string, requestValue, responseValue any) error {
	if c == nil || c.baseURL == "" || c.http == nil {
		return fmt.Errorf("%w: supervisor URL and client are required", workstore.ErrInvalidRequest)
	}
	if err := validateSupervisorBaseURL(c.baseURL); err != nil {
		return err
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("send supervisor request: %w", err)
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, supervisorMaxBytes))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var wire supervisorWireError
		if err := decoder.Decode(&wire); err != nil {
			return fmt.Errorf("supervisor status %d", response.StatusCode)
		}
		return mapSupervisorWireError(wire.Code)
	}
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(responseValue); err != nil {
		return fmt.Errorf("decode supervisor response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("supervisor response contains trailing data")
	}
	return nil
}

func validateSupervisorBaseURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Host == "" ||
		parsed.Port() == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: supervisor URL must be a plain loopback HTTP origin", workstore.ErrInvalidRequest)
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%w: supervisor URL must use a loopback IP literal", workstore.ErrInvalidRequest)
	}
	return nil
}

func mapSupervisorWireError(code string) error {
	switch code {
	case "stale_owner":
		return workstore.ErrStaleOwner
	case "session_canceled":
		return workstore.ErrSessionCanceled
	case "session_not_found":
		return workstore.ErrSessionNotFound
	case "invalid_request":
		return workstore.ErrInvalidRequest
	case "execution_unavailable":
		return errSupervisorExecutionUnavailable
	case "request_ended":
		return context.Canceled
	default:
		return errors.New("supervisor request failed")
	}
}
