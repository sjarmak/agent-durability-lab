package temporalagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/failureinject"
	"github.com/sjarmak/temporal_projects/internal/workstore"
	"go.temporal.io/sdk/activity"
)

const activityPollInterval = 100 * time.Millisecond

type Activities struct {
	StorePath     string
	AgentBinary   string
	BarrierURL    string
	RunDirectory  string
	WorkerID      string
	AgentBuild    string
	SignalProcess func(agentprocess.ControlRequest) (agentprocess.ControlResult, error)
}

func (a Activities) CancelAgent(ctx context.Context, input CancelActivityInput) (CancelActivityResult, error) {
	if a.StorePath == "" || input.SessionID == "" || input.RequestID == "" {
		return CancelActivityResult{}, errors.New("cancellation Activity requires store, session, and request identities")
	}
	store, err := workstore.Open(a.StorePath)
	if err != nil {
		return CancelActivityResult{}, fmt.Errorf("open application work store for cancellation: %w", err)
	}
	decision, err := store.CancelSession(ctx, workstore.CancelRequest{
		SessionID: input.SessionID, RequestID: input.RequestID,
	})
	if err != nil {
		return CancelActivityResult{}, fmt.Errorf("commit logical session cancellation: %w", err)
	}
	result := CancelActivityResult{Action: decision.Action, Delivery: CancellationDeliveryNotRequired}
	if decision.Action == workstore.CancelActionAlreadyCompleted || decision.Cancellation == nil {
		return result, nil
	}
	target := decision.Cancellation.Target
	if target.Process.PID <= 0 || target.Process.StartIdentity == "" || target.Process.ProcessGroupID <= 0 {
		return result, nil
	}
	controlTarget := agentprocess.ControlTarget{
		SessionID: target.SessionID, Generation: target.Generation, OwnerTokenHash: target.OwnerTokenHash,
		Leader: agentprocess.ProcessIdentity{
			PID: target.Process.PID, StartIdentity: target.Process.StartIdentity,
			ProcessGroupID: target.Process.ProcessGroupID,
		},
	}
	snapshot, err := store.Snapshot(ctx, input.SessionID)
	if err != nil {
		return CancelActivityResult{}, fmt.Errorf("read process-tree evidence: %w", err)
	}
	controlTarget.Members, err = processTreeMembers(snapshot, controlTarget)
	if err != nil {
		return CancelActivityResult{}, err
	}
	controlRequest := agentprocess.ControlRequest{
		Target: controlTarget, Scope: agentprocess.ScopeProcessTree, Signal: agentprocess.SignalTerminate,
	}
	if err := store.RecordObservation(ctx, processControlEvent(
		"executor_stop_delivery_attempted", input.SessionID, controlRequest, "",
	)); err != nil {
		return CancelActivityResult{}, fmt.Errorf("record executor stop attempt: %w", err)
	}
	signalProcess := a.SignalProcess
	if signalProcess == nil {
		signalProcess = agentprocess.Signal
	}
	controlResult, signalErr := signalProcess(controlRequest)
	if signalErr != nil {
		result.Delivery = CancellationDeliveryFailed
		if err := store.RecordObservation(ctx, processControlEvent(
			"executor_stop_delivery_failed", input.SessionID, controlRequest, signalErr.Error(),
		)); err != nil {
			return CancelActivityResult{}, fmt.Errorf("record executor stop failure: %w", err)
		}
		return result, nil
	}
	result.Delivery = CancellationDeliverySent
	event := processControlEvent("executor_stop_delivery_sent", input.SessionID, controlRequest, "")
	event.Details["method"] = string(controlResult.Method)
	event.Details["requested_at"] = controlResult.RequestedAt.Format(time.RFC3339Nano)
	if err := store.RecordObservation(ctx, event); err != nil {
		return CancelActivityResult{}, fmt.Errorf("record executor stop delivery: %w", err)
	}
	return result, nil
}

func processTreeMembers(snapshot workstore.Snapshot, target agentprocess.ControlTarget) ([]agentprocess.ProcessIdentity, error) {
	members := []agentprocess.ProcessIdentity{target.Leader}
	seen := map[int]bool{target.Leader.PID: true}
	for _, event := range snapshot.Events {
		if event.Kind != "tool_child_registered" || event.Generation != target.Generation ||
			event.OwnerTokenHash != target.OwnerTokenHash {
			continue
		}
		processGroupID, err := strconv.Atoi(event.Details["process_group_id"])
		if err != nil || event.PID <= 0 || event.Details["process_start"] == "" {
			return nil, fmt.Errorf("invalid tool-child process evidence at event %d", event.Sequence)
		}
		if processGroupID != target.Leader.ProcessGroupID {
			return nil, fmt.Errorf(
				"tool-child event %d process group %d does not match leader group %d",
				event.Sequence, processGroupID, target.Leader.ProcessGroupID,
			)
		}
		if seen[event.PID] {
			continue
		}
		seen[event.PID] = true
		members = append(members, agentprocess.ProcessIdentity{
			PID: event.PID, StartIdentity: event.Details["process_start"], ProcessGroupID: processGroupID,
		})
	}
	return members, nil
}

func processControlEvent(kind, sessionID string, request agentprocess.ControlRequest, failure string) workstore.Event {
	event := workstore.Event{
		Kind: kind, SessionID: sessionID, Generation: request.Target.Generation,
		OwnerTokenHash: request.Target.OwnerTokenHash, PID: request.Target.Leader.PID,
		Details: map[string]string{
			"process_start":    request.Target.Leader.StartIdentity,
			"process_group_id": fmt.Sprint(request.Target.Leader.ProcessGroupID),
			"scope":            string(request.Scope), "signal": string(request.Signal),
		},
	}
	if failure != "" {
		event.Details["error"] = failure
	}
	return event
}

type HeartbeatDetails struct {
	SessionID      string `json:"session_id"`
	Generation     uint64 `json:"generation"`
	OwnerTokenHash string `json:"owner_token_hash"`
	Phase          string `json:"phase"`
}

func (a Activities) RunAgent(ctx context.Context, input ActivityInput) (workstore.Outcome, error) {
	if err := a.validate(input); err != nil {
		return workstore.Outcome{}, err
	}
	store, err := workstore.Open(a.StorePath)
	if err != nil {
		return workstore.Outcome{}, fmt.Errorf("open application work store: %w", err)
	}
	info := activity.GetInfo(ctx)
	ownerToken, err := newOwnerToken()
	if err != nil {
		return workstore.Outcome{}, err
	}
	decision, err := store.StartOrAttach(ctx, workstore.StartRequest{
		SessionID: input.SessionID, Mode: input.Mode, CandidateOwner: ownerToken,
		WorkerID: a.WorkerID, AgentBuild: a.AgentBuild, Attempt: info.Attempt,
		ReplaceOwner:         input.Mode == workstore.ModeFenced && input.ReplaceOwnerOnRetry && info.Attempt > 1,
		ReplacePendingLaunch: input.Mode == workstore.ModeFenced && input.ReplacePendingLaunchOnRetry && info.Attempt > 1,
	})
	if err != nil {
		return workstore.Outcome{}, fmt.Errorf("resolve agent session: %w", err)
	}
	if decision.Action == workstore.ActionComplete {
		return *decision.Outcome, nil
	}
	if decision.Action == workstore.ActionLaunch {
		if input.BlockAttempt1AfterLaunchDecision && info.Attempt == 1 {
			if err := a.blockAfterLaunchDecision(ctx, store, decision.Lease, info.Attempt); err != nil {
				return workstore.Outcome{}, err
			}
		}
		if err := a.launchAgent(
			decision.Lease, store.Path(), input.BlockAttempt1BeforeRegistration && info.Attempt == 1,
			input.SpawnToolChild,
		); err != nil {
			return workstore.Outcome{}, err
		}
	}

	if input.BlockAttempt1BeforeHeartbeat && info.Attempt == 1 {
		if err := a.blockBeforeFirstHeartbeat(ctx, store, decision.Lease, info.Attempt); err != nil {
			return workstore.Outcome{}, err
		}
	}
	return a.waitForOutcome(ctx, store, decision.Lease, info.Attempt)
}

func (a Activities) launchAgent(
	lease workstore.Lease,
	storePath string,
	blockBeforeRegistration, spawnToolChild bool,
) error {
	tokenHash := workstore.HashToken(lease.OwnerToken)
	actorID := fmt.Sprintf("agent/%s/g%d/%s", lease.SessionID, lease.Generation, tokenHash[:12])
	launcher := agentprocess.NewLauncher(a.AgentBinary, filepath.Join(a.RunDirectory, sessionDirectoryName(lease.SessionID)))
	_, err := launcher.Launch(agentprocess.LaunchRequest{
		StorePath: storePath, BarrierURL: a.BarrierURL,
		Config: agentsim.Config{
			Lease: lease, ActorID: actorID,
			BlockBeforeRegistration: blockBeforeRegistration,
			SpawnToolChild:          spawnToolChild,
			Effect: workstore.Effect{
				ID:    fmt.Sprintf("%s/tool-write/g%d", lease.SessionID, lease.Generation),
				Value: fmt.Sprintf("mutation by generation %d", lease.Generation),
			},
			Outcome: workstore.Outcome{
				Value:       fmt.Sprintf("outcome/%s/g%d", lease.SessionID, lease.Generation),
				ArtifactRef: fmt.Sprintf("artifact://%s/g%d", lease.SessionID, lease.Generation),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("launch detached agent: %w", err)
	}
	return nil
}

func sessionDirectoryName(sessionID string) string {
	return "session-" + workstore.HashToken(sessionID)[:24]
}

func (a Activities) blockBeforeFirstHeartbeat(
	ctx context.Context,
	store *workstore.Store,
	lease workstore.Lease,
	attempt int32,
) error {
	return a.blockAtActivityBoundary(ctx, store, lease, attempt,
		"activity-before-first-heartbeat", "activity_before_first_heartbeat")
}

func (a Activities) blockAfterLaunchDecision(
	ctx context.Context,
	store *workstore.Store,
	lease workstore.Lease,
	attempt int32,
) error {
	return a.blockAtActivityBoundary(ctx, store, lease, attempt,
		"activity-after-launch-decision", "activity_after_launch_decision")
}

func (a Activities) blockAtActivityBoundary(
	ctx context.Context,
	store *workstore.Store,
	lease workstore.Lease,
	attempt int32,
	pointPrefix, eventKind string,
) error {
	point := fmt.Sprintf("%s/%d", pointPrefix, attempt)
	tokenHash := workstore.HashToken(lease.OwnerToken)
	if err := store.RecordObservation(ctx, workstore.Event{
		Kind: eventKind, SessionID: lease.SessionID,
		Generation: lease.Generation, OwnerTokenHash: tokenHash,
		WorkerID: a.WorkerID, Attempt: attempt,
	}); err != nil {
		return fmt.Errorf("record Activity boundary %q: %w", point, err)
	}
	arrival := failureinject.Arrival{
		ID: fmt.Sprintf("%s:%s", a.WorkerID, point), Point: point,
		SessionID: lease.SessionID, OwnerTokenHash: tokenHash,
		Generation: lease.Generation, ActorID: a.WorkerID,
	}
	if err := failureinject.NewClient(a.BarrierURL).Arrive(ctx, arrival); err != nil {
		return fmt.Errorf("wait at Activity boundary %q: %w", point, err)
	}
	return nil
}

func (a Activities) waitForOutcome(
	ctx context.Context,
	store *workstore.Store,
	lease workstore.Lease,
	attempt int32,
) (workstore.Outcome, error) {
	tokenHash := workstore.HashToken(lease.OwnerToken)
	ticker := time.NewTicker(activityPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := store.Snapshot(ctx, lease.SessionID)
		if err != nil {
			return workstore.Outcome{}, fmt.Errorf("observe agent session: %w", err)
		}
		if snapshot.Outcome != nil {
			return *snapshot.Outcome, nil
		}
		select {
		case <-ctx.Done():
			return workstore.Outcome{}, context.Cause(ctx)
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, HeartbeatDetails{
				SessionID: lease.SessionID, Generation: lease.Generation,
				OwnerTokenHash: tokenHash, Phase: "waiting-for-agent-outcome",
			})
			if err := store.RecordObservation(ctx, workstore.Event{
				Kind: "activity_heartbeat_recorded", SessionID: lease.SessionID,
				Generation: lease.Generation, OwnerTokenHash: tokenHash,
				WorkerID: a.WorkerID, Attempt: attempt,
			}); err != nil {
				return workstore.Outcome{}, fmt.Errorf("record Activity heartbeat evidence: %w", err)
			}
		}
	}
}

func (a Activities) validate(input ActivityInput) error {
	if a.StorePath == "" || a.AgentBinary == "" || a.BarrierURL == "" || a.RunDirectory == "" || a.WorkerID == "" || a.AgentBuild == "" {
		return errors.New("activity requires store, agent binary, barrier, run directory, worker, and build identities")
	}
	if input.SessionID == "" || !input.Mode.Valid() {
		return errors.New("activity requires a session ID and valid mode")
	}
	if input.ReplaceOwnerOnRetry && input.Mode != workstore.ModeFenced {
		return errors.New("replacement on retry requires fenced mode")
	}
	if input.ReplacePendingLaunchOnRetry && input.Mode != workstore.ModeFenced {
		return errors.New("pending launch recovery requires fenced mode")
	}
	if input.ReplaceOwnerOnRetry && input.ReplacePendingLaunchOnRetry {
		return errors.New("replacement policies are mutually exclusive")
	}
	return nil
}

func newOwnerToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate owner token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
