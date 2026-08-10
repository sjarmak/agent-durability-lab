// Package agent adapts the lab's hermetic detached process to the topology
// protocol without exposing orchestration topology to the work implementation.
package agent

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
	"github.com/sjarmak/temporal_projects/internal/agentsim"
	"github.com/sjarmak/temporal_projects/internal/workstore"
)

type Request struct {
	Manifest         protocol.Manifest `json:"manifest"`
	WorkItemID       string            `json:"work_item_id"`
	Lease            workstore.Lease   `json:"lease"`
	ParentWorkflowID string            `json:"parent_workflow_id"`
	ParentRunID      string            `json:"parent_run_id"`
	ChildWorkflowID  string            `json:"child_workflow_id,omitempty"`
	ChildRunID       string            `json:"child_run_id,omitempty"`
	ActivityID       string            `json:"activity_id"`
	ActivityAttempt  int               `json:"activity_attempt"`
	WorkerID         string            `json:"worker_id"`
	WorkerPID        int               `json:"worker_pid"`
	StorePath        string            `json:"store_path"`
	BarrierURL       string            `json:"barrier_url"`
	EffectValue      string            `json:"effect_value"`
	OutcomeValue     string            `json:"outcome_value"`
	// BlockBeforeRegistration exposes the exact post-exec/pre-registration
	// crash boundary to the recovery-dynamics harness.
	BlockBeforeRegistration bool `json:"block_before_registration,omitempty"`
	// BypassAuthorityForEffect is the explicit unsafe supersession control.
	BypassAuthorityForEffect bool `json:"bypass_authority_for_effect,omitempty"`
}

type Launched struct {
	Process  agentprocess.Process `json:"process"`
	Identity protocol.Identity    `json:"identity"`
}

type Launcher struct {
	process      *agentprocess.Launcher
	runDirectory string
	workRoot     string
}

func NewLauncher(binary, runDirectory, workRoot string) *Launcher {
	return &Launcher{
		process: agentprocess.NewLauncher(binary, runDirectory), runDirectory: runDirectory, workRoot: workRoot,
	}
}

func (l *Launcher) Launch(ctx context.Context, request Request) (Launched, error) {
	if err := request.validate(); err != nil {
		return Launched{}, err
	}
	if ctx == nil {
		return Launched{}, fmt.Errorf("%w: nil launch context", protocol.ErrInvalidEvidence)
	}
	if l == nil || l.process == nil || l.workRoot == "" || l.runDirectory == "" {
		return Launched{}, fmt.Errorf("%w: agent launcher", protocol.ErrInvalidEvidence)
	}
	storePath, err := l.validateEnvironment(request)
	if err != nil {
		return Launched{}, err
	}
	request.StorePath = storePath
	identity := identityForRequest(request, "launch-pending")
	if err := identity.Validate(); err != nil {
		return Launched{}, err
	}
	select {
	case <-ctx.Done():
		return Launched{}, ctx.Err()
	default:
	}
	process, err := l.process.Launch(agentprocess.LaunchRequest{
		StorePath:  request.StorePath,
		BarrierURL: request.BarrierURL,
		Config: agentsim.Config{
			Lease:                   request.Lease,
			ActorID:                 "agent/" + request.WorkItemID,
			BlockBeforeRegistration: request.BlockBeforeRegistration,
			Effect: workstore.Effect{
				ID:    request.Manifest.LogicalOperationID + "/" + request.WorkItemID + "/effect",
				Value: request.EffectValue,
			},
			Outcome:                  workstore.Outcome{Value: request.OutcomeValue},
			BypassAuthorityForEffect: request.BypassAuthorityForEffect,
		},
	})
	if err != nil {
		return Launched{}, err
	}
	identity.ProcessIdentity = fmt.Sprintf("pid:%d/start:%s", process.PID, process.StartIdentity)
	return Launched{Process: process, Identity: identity}, nil
}

func identityForRequest(request Request, processIdentity string) protocol.Identity {
	return protocol.Identity{
		ProtocolVersion: request.Manifest.ProtocolVersion,
		RunID:           request.Manifest.RunID, PairID: request.Manifest.PairID, ScheduleBlockID: request.Manifest.ScheduleBlockID,
		TrackerBeadID: request.Manifest.TrackerBeadID, Topology: request.Manifest.Topology, Case: request.Manifest.Case,
		Boundary: request.Manifest.Boundary, Probe: request.Manifest.Probe, Fanout: request.Manifest.Fanout,
		LogicalOperationID: request.Manifest.LogicalOperationID, WorkItemID: request.WorkItemID,
		Generation: request.Lease.Generation, CapabilityHash: workstore.HashToken(request.Lease.OwnerToken),
		ParentWorkflowID: request.ParentWorkflowID, ParentRunID: request.ParentRunID,
		ChildWorkflowID: request.ChildWorkflowID, ChildRunID: request.ChildRunID,
		ActivityID: request.ActivityID, ActivityAttempt: request.ActivityAttempt,
		WorkerID: request.WorkerID, WorkerPID: request.WorkerPID,
		ProcessIdentity: processIdentity,
	}

}

func (l *Launcher) validateEnvironment(request Request) (string, error) {
	absoluteRoot, err := filepath.Abs(l.workRoot)
	if err != nil {
		return "", err
	}
	resolvedRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("%w: resolve work root: %v", protocol.ErrInvalidEvidence, err)
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return "", fmt.Errorf("%w: work root", protocol.ErrInvalidEvidence)
	}
	resolvedStore, err := confinedExistingRegular(resolvedRoot, request.StorePath)
	if err != nil {
		return "", err
	}
	if err := confinedRunDirectory(absoluteRoot, resolvedRoot, l.runDirectory); err != nil {
		return "", err
	}
	if err := validateBarrierURL(request.BarrierURL); err != nil {
		return "", err
	}
	return resolvedStore, nil
}

func confinedExistingRegular(root, candidate string) (string, error) {
	absolute, err := filepath.Abs(candidate)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || !withinRoot(root, resolved) {
		return "", fmt.Errorf("%w: store path outside work root", protocol.ErrInvalidEvidence)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: store is not a regular file", protocol.ErrInvalidEvidence)
	}
	return resolved, nil
}

func confinedRunDirectory(absoluteRoot, resolvedRoot, candidate string) error {
	absolute, err := filepath.Abs(candidate)
	if err != nil || !withinRoot(absoluteRoot, absolute) {
		return fmt.Errorf("%w: process directory outside work root", protocol.ErrInvalidEvidence)
	}
	if err := os.MkdirAll(absolute, 0o750); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || !withinRoot(resolvedRoot, resolved) {
		return fmt.Errorf("%w: process directory outside work root", protocol.ErrInvalidEvidence)
	}
	return nil
}

func withinRoot(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != "." && !filepath.IsAbs(relative) && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateBarrierURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Host == "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: barrier URL", protocol.ErrInvalidEvidence)
	}
	host := net.ParseIP(parsed.Hostname())
	if host == nil || !host.IsLoopback() {
		return fmt.Errorf("%w: barrier must use a loopback IP", protocol.ErrInvalidEvidence)
	}
	return nil
}

func (r Request) validate() error {
	if err := r.Manifest.Validate(); err != nil {
		return err
	}
	if r.WorkItemID == "" || r.Lease.SessionID == "" || r.Lease.Generation == 0 || r.Lease.OwnerToken == "" ||
		r.ParentWorkflowID == "" || r.ParentRunID == "" || r.ActivityID == "" || r.ActivityAttempt < 1 ||
		r.WorkerID == "" || r.WorkerPID < 1 || r.StorePath == "" || r.BarrierURL == "" || r.EffectValue == "" || r.OutcomeValue == "" {
		return fmt.Errorf("%w: hermetic agent request", protocol.ErrInvalidEvidence)
	}
	if r.Lease.SessionID != r.Manifest.LogicalOperationID+"/"+r.WorkItemID {
		return fmt.Errorf("%w: stable agent session identity", protocol.ErrInvalidEvidence)
	}
	if r.Manifest.Topology == protocol.TopologyDirectActivity && (r.ChildWorkflowID != "" || r.ChildRunID != "") {
		return fmt.Errorf("%w: direct agent request names child", protocol.ErrInvalidEvidence)
	}
	if r.Manifest.Topology == protocol.TopologyChildWorkflow && (r.ChildWorkflowID == "" || r.ChildRunID == "") {
		return fmt.Errorf("%w: child agent request lacks child identity", protocol.ErrInvalidEvidence)
	}
	if r.BypassAuthorityForEffect && r.Manifest.Probe != protocol.ProbeUnsafe {
		return fmt.Errorf("%w: authority bypass outside unsafe control", protocol.ErrInvalidEvidence)
	}
	return nil
}
