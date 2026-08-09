package abalive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

type Action string

const (
	ActionEffect          Action = "effect"
	ActionCompletion      Action = "completion"
	ActionEpochComplete   Action = "epoch_complete"
	ActionOutcome         Action = "outcome"
	ActionAcknowledgement Action = "acknowledgement"
	ActionStop            Action = "stop"
)

func (a Action) valid() bool {
	return a == ActionEffect || a == ActionCompletion || a == ActionEpochComplete || a == ActionOutcome || a == ActionAcknowledgement || a == ActionStop
}

type LaunchRequest struct {
	Endpoint           string   `json:"endpoint"`
	RunID              string   `json:"run_id"`
	LogicalOperationID string   `json:"logical_operation_id"`
	WorkItemID         string   `json:"work_item_id"`
	OwnerID            string   `json:"owner_id"`
	Generation         uint64   `json:"generation"`
	Capability         string   `json:"capability"`
	AttemptID          string   `json:"attempt_id"`
	ParentAttemptID    string   `json:"parent_attempt_id,omitempty"`
	RetryOrdinal       int      `json:"retry_ordinal"`
	RetryCause         string   `json:"retry_cause,omitempty"`
	WorkerID           string   `json:"worker_id"`
	Actions            []Action `json:"actions"`
}

func (r LaunchRequest) validate() error {
	endpoint, err := url.Parse(r.Endpoint)
	if err != nil || endpoint.Scheme != "http" || !loopbackHost(endpoint.Hostname()) {
		return errors.New("ABA client endpoint must be loopback HTTP")
	}
	if r.RunID == "" || r.LogicalOperationID == "" || r.WorkItemID == "" || r.OwnerID == "" || r.Generation == 0 ||
		r.Capability == "" || r.AttemptID == "" || r.RetryOrdinal < 1 || r.WorkerID == "" || len(r.Actions) == 0 {
		return errors.New("ABA client requires complete authority, attempt, and action identity")
	}
	if r.RetryOrdinal == 1 && r.ParentAttemptID != "" || r.RetryOrdinal > 1 && (r.ParentAttemptID == "" || r.RetryCause == "") {
		return errors.New("ABA client retry identity is inconsistent")
	}
	for _, action := range r.Actions {
		if !action.valid() {
			return fmt.Errorf("unsupported ABA action %q", action)
		}
	}
	return nil
}

type ActionRequest struct {
	RunID              string `json:"run_id"`
	LogicalOperationID string `json:"logical_operation_id"`
	WorkItemID         string `json:"work_item_id"`
	OwnerID            string `json:"owner_id"`
	Generation         uint64 `json:"generation"`
	Capability         string `json:"capability"`
	AttemptID          string `json:"attempt_id"`
	ParentAttemptID    string `json:"parent_attempt_id,omitempty"`
	RetryOrdinal       int    `json:"retry_ordinal"`
	RetryCause         string `json:"retry_cause,omitempty"`
	WorkerID           string `json:"worker_id"`
	ProcessIdentity    string `json:"process_identity"`
	RequestID          string `json:"request_id"`
	Action             Action `json:"action"`
}

type ActionResponse struct {
	RequestID string `json:"request_id"`
	Action    Action `json:"action"`
	Accepted  bool   `json:"accepted"`
	Reason    string `json:"reason"`
}

type ClientResult struct {
	OwnerID         string           `json:"owner_id"`
	Generation      uint64           `json:"generation"`
	ProcessIdentity string           `json:"process_identity"`
	Responses       []ActionResponse `json:"responses"`
}

func RunClient(ctx context.Context, request LaunchRequest) (ClientResult, error) {
	if err := request.validate(); err != nil {
		return ClientResult{}, err
	}
	start, err := agentprocess.CurrentProcessStartIdentity()
	if err != nil {
		return ClientResult{}, err
	}
	processIdentity := fmt.Sprintf("pid:%d:start:%s", os.Getpid(), start)
	result := ClientResult{OwnerID: request.OwnerID, Generation: request.Generation, ProcessIdentity: processIdentity}
	client := &http.Client{}
	for index, action := range request.Actions {
		actionRequest := ActionRequest{
			RunID: request.RunID, LogicalOperationID: request.LogicalOperationID, WorkItemID: request.WorkItemID,
			OwnerID: request.OwnerID, Generation: request.Generation, Capability: request.Capability,
			AttemptID: request.AttemptID, ParentAttemptID: request.ParentAttemptID,
			RetryOrdinal: request.RetryOrdinal, RetryCause: request.RetryCause, WorkerID: request.WorkerID,
			ProcessIdentity: processIdentity, RequestID: fmt.Sprintf("%s-%s-%d", request.AttemptID, action, index+1), Action: action,
		}
		response, err := postAction(ctx, client, request.Endpoint, actionRequest)
		if err != nil {
			return ClientResult{}, err
		}
		result.Responses = append(result.Responses, response)
	}
	return result, nil
}

func postAction(ctx context.Context, client *http.Client, endpoint string, action ActionRequest) (ActionResponse, error) {
	body, err := json.Marshal(action)
	if err != nil {
		return ActionResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/v1/actions", bytes.NewReader(body))
	if err != nil {
		return ActionResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return ActionResponse{}, fmt.Errorf("send ABA action %s: %w", action.Action, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return ActionResponse{}, fmt.Errorf("ABA action %s status %d: %s", action.Action, response.StatusCode, strings.TrimSpace(string(message)))
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	decoder.DisallowUnknownFields()
	var result ActionResponse
	if err := decoder.Decode(&result); err != nil {
		return ActionResponse{}, fmt.Errorf("decode ABA action %s: %w", action.Action, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ActionResponse{}, errors.New("ABA action response contains trailing data")
	}
	if result.RequestID != action.RequestID || result.Action != action.Action || result.Reason == "" {
		return ActionResponse{}, errors.New("ABA action response identity is inconsistent")
	}
	return result, nil
}

func ReadLaunchRequest(path string) (LaunchRequest, error) {
	if path == "" {
		return LaunchRequest{}, errors.New("ABA launch request path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return LaunchRequest{}, err
	}
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var request LaunchRequest
	decodeErr := decoder.Decode(&request)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	closeErr := file.Close()
	removeErr := os.Remove(path)
	if decodeErr != nil || !errors.Is(trailingErr, io.EOF) || closeErr != nil || removeErr != nil {
		return LaunchRequest{}, errors.Join(decodeErr, trailingErrIfUnexpected(trailingErr), closeErr, removeErr)
	}
	return request, request.validate()
}

func trailingErrIfUnexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("ABA launch request contains trailing data")
	}
	return err
}

func loopbackHost(host string) bool {
	return host == "127.0.0.1" || host == "::1" || strings.EqualFold(host, "localhost")
}
