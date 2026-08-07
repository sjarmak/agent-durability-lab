package temporalagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/temporalio-labs/agent-durability-lab/internal/agentprocess"
	"github.com/temporalio-labs/agent-durability-lab/internal/agentsim"
	"github.com/temporalio-labs/agent-durability-lab/internal/failureinject"
	"github.com/temporalio-labs/agent-durability-lab/internal/workstore"
	"go.temporal.io/sdk/activity"
)

const activityPollInterval = 100 * time.Millisecond

type Activities struct {
	StorePath    string
	AgentBinary  string
	BarrierURL   string
	RunDirectory string
	WorkerID     string
	AgentBuild   string
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
		); err != nil {
			return workstore.Outcome{}, err
		}
	}

	if input.BlockAttempt1BeforeHeartbeat && info.Attempt == 1 {
		if err := a.blockBeforeFirstHeartbeat(ctx, store, decision.Lease, info.Attempt); err != nil {
			return workstore.Outcome{}, err
		}
	}
	return a.waitForOutcome(ctx, store, decision.Lease)
}

func (a Activities) launchAgent(lease workstore.Lease, storePath string, blockBeforeRegistration bool) error {
	tokenHash := workstore.HashToken(lease.OwnerToken)
	actorID := fmt.Sprintf("agent/%s/g%d/%s", lease.SessionID, lease.Generation, tokenHash[:12])
	launcher := agentprocess.NewLauncher(a.AgentBinary, filepath.Join(a.RunDirectory, sessionDirectoryName(lease.SessionID)))
	_, err := launcher.Launch(agentprocess.LaunchRequest{
		StorePath: storePath, BarrierURL: a.BarrierURL,
		Config: agentsim.Config{
			Lease: lease, ActorID: actorID,
			BlockBeforeRegistration: blockBeforeRegistration,
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

func (a Activities) waitForOutcome(ctx context.Context, store *workstore.Store, lease workstore.Lease) (workstore.Outcome, error) {
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
