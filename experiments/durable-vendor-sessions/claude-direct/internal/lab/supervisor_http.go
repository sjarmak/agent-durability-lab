package lab

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"unicode"

	"github.com/sjarmak/temporal_projects/internal/workstore"
)

const (
	supervisorStartPath          = "/v1/turns/start-or-attach"
	supervisorCancelPath         = "/v1/sessions/cancel"
	supervisorEffectPath         = "/v1/effects/commit"
	supervisorHealthPath         = "/healthz"
	supervisorMaxRequestBytes    = 64 << 10
	supervisorMaxResponseBytes   = 64 << 10
	supervisorMaxIdentifierBytes = 512
)

type supervisorCancelRequest struct {
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

type supervisorCancelReceipt struct {
	Action         workstore.CancelAction `json:"action"`
	Generation     uint64                 `json:"generation,omitempty"`
	OwnerTokenHash string                 `json:"owner_token_hash,omitempty"`
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
	Code    string `json:"code"`
	Message string `json:"message"`
}

type supervisorHTTPServer struct {
	supervisor *turnSupervisor
}

func newSupervisorHandler(supervisor *turnSupervisor) http.Handler {
	server := &supervisorHTTPServer{supervisor: supervisor}
	mux := http.NewServeMux()
	mux.HandleFunc(supervisorHealthPath, server.health)
	mux.HandleFunc(supervisorStartPath, server.startOrAttach)
	mux.HandleFunc(supervisorCancelPath, server.cancel)
	mux.HandleFunc(supervisorEffectPath, server.commitEffect)
	return mux
}

func (s *supervisorHTTPServer) health(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		writeSupervisorError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *supervisorHTTPServer) startOrAttach(response http.ResponseWriter, request *http.Request) {
	if !requireSupervisorPOST(response, request) {
		return
	}
	var start supervisorStartRequest
	if err := decodeSupervisorJSON(response, request, &start); err != nil {
		writeSupervisorDecodeError(response, err)
		return
	}
	if !validSupervisorStart(start) {
		writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	receipt, err := s.supervisor.StartOrAttach(request.Context(), start)
	if err != nil {
		writeSupervisorOperationError(response, err)
		return
	}
	writeSupervisorJSON(response, http.StatusOK, receipt)
}

func (s *supervisorHTTPServer) cancel(response http.ResponseWriter, request *http.Request) {
	if !requireSupervisorPOST(response, request) {
		return
	}
	var cancel supervisorCancelRequest
	if err := decodeSupervisorJSON(response, request, &cancel); err != nil {
		writeSupervisorDecodeError(response, err)
		return
	}
	if !validSupervisorIdentifier(cancel.SessionID) || !validSupervisorIdentifier(cancel.RequestID) {
		writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	decision, err := s.supervisor.Cancel(request.Context(), cancel.SessionID, cancel.RequestID)
	if err != nil {
		writeSupervisorOperationError(response, err)
		return
	}
	receipt := supervisorCancelReceipt{Action: decision.Action}
	if decision.Cancellation != nil {
		receipt.Generation = decision.Cancellation.Generation
		receipt.OwnerTokenHash = decision.Cancellation.OwnerTokenHash
	}
	writeSupervisorJSON(response, http.StatusOK, receipt)
}

func (s *supervisorHTTPServer) commitEffect(response http.ResponseWriter, request *http.Request) {
	if !requireSupervisorPOST(response, request) {
		return
	}
	var effect supervisorEffectRequest
	if err := decodeSupervisorJSON(response, request, &effect); err != nil {
		writeSupervisorDecodeError(response, err)
		return
	}
	if !validSupervisorIdentifier(effect.SessionID) || effect.Generation == 0 ||
		!validSupervisorIdentifier(effect.OwnerCapability) ||
		!validSupervisorIdentifier(effect.EffectID) || effect.Value == "" || len(effect.Value) > 8<<10 {
		writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	lease := workstore.Lease{
		SessionID: effect.SessionID, Generation: effect.Generation, OwnerToken: effect.OwnerCapability,
	}
	if err := s.supervisor.store.CommitEffectOnce(request.Context(), lease, workstore.Effect{
		ID: effect.EffectID, Value: effect.Value,
	}); err != nil {
		writeSupervisorOperationError(response, err)
		return
	}
	writeSupervisorJSON(response, http.StatusOK, supervisorEffectReceipt{
		Accepted: true, Generation: effect.Generation, OwnerTokenHash: workstore.HashToken(effect.OwnerCapability),
	})
}

func requireSupervisorPOST(response http.ResponseWriter, request *http.Request) bool {
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeSupervisorError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return false
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeSupervisorError(response, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return false
	}
	return true
}

func decodeSupervisorJSON(response http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(response, request.Body, supervisorMaxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validSupervisorStart(request supervisorStartRequest) bool {
	return validSupervisorIdentifier(request.SessionID) &&
		validSupervisorIdentifier(request.WorkerID) &&
		validOptionalSupervisorIdentifier(request.AgentBuild) &&
		validOptionalSupervisorIdentifier(request.LogicalTurnID) &&
		validOptionalSupervisorIdentifier(request.LogicalEffectID) &&
		validOptionalSupervisorIdentifier(request.SelectedVendorSessionID) &&
		request.Attempt > 0
}

func validSupervisorIdentifier(value string) bool {
	return value != "" && len(value) <= supervisorMaxIdentifierBytes && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validOptionalSupervisorIdentifier(value string) bool {
	return value == "" || validSupervisorIdentifier(value)
}

func writeSupervisorDecodeError(response http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeSupervisorError(response, http.StatusRequestEntityTooLarge, "request_too_large")
		return
	}
	writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
}

func writeSupervisorOperationError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workstore.ErrInvalidRequest):
		writeSupervisorError(response, http.StatusBadRequest, "invalid_request")
	case errors.Is(err, workstore.ErrSessionNotFound):
		writeSupervisorError(response, http.StatusNotFound, "session_not_found")
	case errors.Is(err, workstore.ErrStaleOwner):
		writeSupervisorError(response, http.StatusConflict, "stale_owner")
	case errors.Is(err, workstore.ErrSessionCanceled):
		writeSupervisorError(response, http.StatusConflict, "session_canceled")
	case errors.Is(err, workstore.ErrEffectConflict):
		writeSupervisorError(response, http.StatusConflict, "effect_conflict")
	case errors.Is(err, errSupervisorExecutionUnavailable):
		writeSupervisorError(response, http.StatusServiceUnavailable, "execution_unavailable")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeSupervisorError(response, http.StatusRequestTimeout, "request_ended")
	default:
		writeSupervisorError(response, http.StatusInternalServerError, "internal_error")
	}
}

func writeSupervisorError(response http.ResponseWriter, status int, code string) {
	writeSupervisorJSON(response, status, supervisorWireError{Code: code, Message: "request rejected"})
}

func writeSupervisorJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

type supervisorClient struct {
	baseURL string
	client  *http.Client
}

func newSupervisorClient(baseURL string, client *http.Client) *supervisorClient {
	if client == nil {
		client = http.DefaultClient
	}
	noRedirect := *client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &supervisorClient{baseURL: strings.TrimRight(baseURL, "/"), client: &noRedirect}
}

func (c *supervisorClient) StartOrAttach(ctx context.Context, request supervisorStartRequest) (supervisorReceipt, error) {
	var receipt supervisorReceipt
	if err := c.post(ctx, supervisorStartPath, request, &receipt); err != nil {
		return supervisorReceipt{}, err
	}
	if err := validateSupervisorStartReceipt(receipt); err != nil {
		return supervisorReceipt{}, err
	}
	return receipt, nil
}

func (c *supervisorClient) Cancel(ctx context.Context, request supervisorCancelRequest) (supervisorCancelReceipt, error) {
	var receipt supervisorCancelReceipt
	if err := c.post(ctx, supervisorCancelPath, request, &receipt); err != nil {
		return supervisorCancelReceipt{}, err
	}
	if err := validateSupervisorCancelReceipt(receipt); err != nil {
		return supervisorCancelReceipt{}, err
	}
	return receipt, nil
}

func (c *supervisorClient) CommitEffect(ctx context.Context, request supervisorEffectRequest) error {
	var receipt supervisorEffectReceipt
	if err := c.post(ctx, supervisorEffectPath, request, &receipt); err != nil {
		return err
	}
	if !receipt.Accepted || receipt.Generation != request.Generation ||
		receipt.OwnerTokenHash != workstore.HashToken(request.OwnerCapability) {
		return errors.New("supervisor effect receipt does not match the requested authority")
	}
	return nil
}

func (c *supervisorClient) post(ctx context.Context, path string, requestValue, responseValue any) error {
	if c == nil || c.client == nil || c.baseURL == "" || ctx == nil {
		return fmt.Errorf("%w: complete supervisor client request is required", workstore.ErrInvalidRequest)
	}
	if err := validateSupervisorBaseURL(c.baseURL); err != nil {
		return fmt.Errorf("%w: %v", workstore.ErrInvalidRequest, err)
	}
	body, err := json.Marshal(requestValue)
	if err != nil {
		return fmt.Errorf("encode supervisor request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("build supervisor request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("call supervisor: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var wireError supervisorWireError
		if err := decodeSupervisorResponse(response, &wireError); err != nil {
			return fmt.Errorf("supervisor returned HTTP %d", response.StatusCode)
		}
		return mapSupervisorWireError(response.StatusCode, wireError.Code)
	}
	if err := decodeSupervisorResponse(response, responseValue); err != nil {
		return fmt.Errorf("decode supervisor response: %w", err)
	}
	return nil
}

func validateSupervisorBaseURL(baseURL string) error {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("supervisor URL must be an origin-only HTTP(S) URL")
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return errors.New("supervisor URL must use a literal loopback address")
	}
	return nil
}

func decodeSupervisorResponse(response *http.Response, target any) error {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("supervisor response is not application/json")
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, supervisorMaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read supervisor response: %w", err)
	}
	if len(encoded) > supervisorMaxResponseBytes {
		return errors.New("supervisor response exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("supervisor response contains trailing data")
	}
	return nil
}

func validateSupervisorStartReceipt(receipt supervisorReceipt) error {
	if receipt.Generation == 0 || !validSHA256Digest(receipt.OwnerTokenHash) ||
		!validSupervisorIdentifier(receipt.Outcome.Value) {
		return errors.New("supervisor start receipt lacks authority or outcome")
	}
	completeExecutionIdentity := validSupervisorIdentifier(receipt.VendorSessionID) &&
		validSupervisorIdentifier(receipt.PhysicalAttemptID) && validSupervisorIdentifier(receipt.ProcessIdentity)
	switch receipt.Action {
	case workstore.ActionLaunch, workstore.ActionAttach:
		if !completeExecutionIdentity {
			return errors.New("supervisor start receipt lacks execution identity")
		}
	case workstore.ActionComplete:
		identityFields := 0
		for _, value := range []string{receipt.VendorSessionID, receipt.PhysicalAttemptID, receipt.ProcessIdentity} {
			if value != "" {
				identityFields++
			}
		}
		if identityFields != 0 && (identityFields != 3 || !completeExecutionIdentity) {
			return errors.New("supervisor terminal receipt has a partial execution identity")
		}
	default:
		return errors.New("supervisor start receipt has an unknown action")
	}
	return nil
}

func validateSupervisorCancelReceipt(receipt supervisorCancelReceipt) error {
	switch receipt.Action {
	case workstore.CancelActionCommitted, workstore.CancelActionAlreadyCanceled:
		if receipt.Generation == 0 || !validSHA256Digest(receipt.OwnerTokenHash) {
			return errors.New("supervisor cancellation receipt lacks revoked authority")
		}
	case workstore.CancelActionAlreadyCompleted:
		if receipt.Generation != 0 || receipt.OwnerTokenHash != "" {
			return errors.New("completed supervisor cancellation receipt names unrelated authority")
		}
	default:
		return errors.New("supervisor cancellation receipt has an unknown action")
	}
	return nil
}

func validSHA256Digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func mapSupervisorWireError(status int, code string) error {
	var sentinel error
	switch code {
	case "invalid_request":
		sentinel = workstore.ErrInvalidRequest
	case "session_not_found":
		sentinel = workstore.ErrSessionNotFound
	case "stale_owner":
		sentinel = workstore.ErrStaleOwner
	case "session_canceled":
		sentinel = workstore.ErrSessionCanceled
	case "effect_conflict":
		sentinel = workstore.ErrEffectConflict
	case "execution_unavailable":
		sentinel = errSupervisorExecutionUnavailable
	default:
		return fmt.Errorf("supervisor returned HTTP %d", status)
	}
	return fmt.Errorf("%w: supervisor returned HTTP %d", sentinel, status)
}
