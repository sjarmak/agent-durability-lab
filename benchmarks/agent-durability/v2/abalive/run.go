package abalive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/evidence"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/internal/agentprocess"
)

type Config struct {
	Root           string
	RunID          string
	Probe          protocol.Probe
	Trial          int
	ClientBinary   string
	AdapterID      string
	AdapterVersion string
	SystemID       string
	Native         []protocol.NativeRecord
	Settings       map[string]string
}

type runningClient struct {
	command         *exec.Cmd
	stdout          bytes.Buffer
	stderr          bytes.Buffer
	processIdentity string
	done            chan error
	waitOnce        sync.Once
	result          ClientResult
	waitErr         error
}

func Run(ctx context.Context, config Config) (string, error) {
	if err := validateConfig(config); err != nil {
		return "", err
	}
	runID := config.RunID
	if runID == "" {
		var err error
		runID, err = randomRunID(config.Probe, config.Trial)
		if err != nil {
			return "", err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	record := newRecorder(runID)
	destination := newDestination(config.Probe, record)
	server := httptest.NewServer(destination.Handler())
	privateDir, err := os.MkdirTemp("", "adl-v2-aba-")
	if err != nil {
		server.Close()
		cancel()
		return "", fmt.Errorf("create ABA private request directory: %w", err)
	}
	if err := os.Chmod(privateDir, 0o700); err != nil {
		server.Close()
		cancel()
		_ = os.RemoveAll(privateDir)
		return "", fmt.Errorf("protect ABA private request directory: %w", err)
	}
	var clients []*runningClient
	defer func() {
		cancel()
		destination.releaseBarrier()
		for _, client := range clients {
			select {
			case <-client.done:
			default:
			}
		}
		server.Close()
		_ = os.RemoveAll(privateDir)
	}()

	record.root()
	capability7 := "capability-a-generation-7-" + runID
	capability8 := "capability-b-generation-8-" + runID
	capability9 := "capability-a-generation-9-" + runID
	destination.setAuthority("A", 7, capability7)

	generation7, err := startClient(runCtx, config.ClientBinary, privateDir, launchRequest(
		server.URL, record, "A", 7, capability7, "attempt-g7", "", 1, "", "worker-a-g7",
		[]Action{ActionEffect, ActionCompletion, ActionAcknowledgement, ActionStop},
	))
	if err != nil {
		return "", err
	}
	clients = append(clients, generation7)
	barrier, err := destination.waitForBarrier(runCtx)
	if err != nil {
		return "", fmt.Errorf("wait for exact generation-7 barrier: %w", err)
	}
	record.markFault(barrier.event, time.Now().UTC())

	destination.setAuthority("B", 8, capability8)
	generation8, err := startClient(runCtx, config.ClientBinary, privateDir, launchRequest(
		server.URL, record, "B", 8, capability8, "attempt-g8", "attempt-g7", 2, "owner_unreachable", "worker-b-g8",
		[]Action{ActionEpochComplete},
	))
	if err != nil {
		return "", err
	}
	clients = append(clients, generation8)
	result8, err := generation8.wait(runCtx)
	if err != nil {
		return "", fmt.Errorf("generation-8 client: %w", err)
	}
	if err := requireDecisions(result8, true, ActionEpochComplete); err != nil {
		return "", err
	}
	record.clientCompleted("B", 8)

	destination.setAuthority("A", 9, capability9)
	generation9, err := startClient(runCtx, config.ClientBinary, privateDir, launchRequest(
		server.URL, record, "A", 9, capability9, "attempt-g9", "attempt-g8", 3, "owner_reacquired", "worker-a-g9",
		[]Action{ActionEffect, ActionOutcome, ActionAcknowledgement},
	))
	if err != nil {
		return "", err
	}
	clients = append(clients, generation9)
	result9, err := generation9.wait(runCtx)
	if err != nil {
		return "", fmt.Errorf("generation-9 client: %w", err)
	}
	if err := requireDecisions(result9, true, ActionEffect, ActionOutcome, ActionAcknowledgement); err != nil {
		return "", err
	}
	record.clientCompleted("A", 9)

	destination.releaseBarrier()
	result7, err := generation7.wait(runCtx)
	if err != nil {
		return "", fmt.Errorf("generation-7 client: %w", err)
	}
	wantStaleAccepted := config.Probe == protocol.ProbeUnsafe
	if err := requireDecisions(result7, wantStaleAccepted, ActionEffect, ActionCompletion, ActionAcknowledgement, ActionStop); err != nil {
		return "", err
	}
	record.clientCompleted("A", 7)
	if err := record.finishFault(barrier.requestID); err != nil {
		return "", err
	}

	clientHash, err := protocol.FileSHA256(config.ClientBinary)
	if err != nil {
		return "", fmt.Errorf("hash ABA client binary: %w", err)
	}
	snapshot := record.snapshot()
	native := append([]protocol.NativeRecord(nil), config.Native...)
	native = append(native, snapshot.nativeJSON()...)
	for index := range native {
		native[index].Sequence = uint64(index + 1)
	}
	adapterID := config.AdapterID
	if adapterID == "" {
		adapterID = "live-common-v2-aba"
	}
	systemID := config.SystemID
	if systemID == "" {
		systemID = "common-live-process-harness"
	}
	settings := map[string]string{
		"barrier": "server-observed-generation-7-effect-request", "case": string(protocol.CaseABAReacquisition),
		"negative_control": string(protocol.ProbeUnsafe), "probe": string(config.Probe),
	}
	for name, value := range config.Settings {
		settings[name] = value
	}
	return evidence.WriteRun(runCtx, config.Root, evidence.Bundle{
		Identity: evidence.RunIdentity{
			RunID: runID, Case: protocol.CaseABAReacquisition, Probe: config.Probe, Trial: config.Trial,
			EpisodeID: record.episodeID, Seed: int64(config.Trial), CohortSize: 1,
		},
		Events: snapshot.events, Authority: snapshot.authority, Destination: snapshot.destination,
		Dependency: snapshot.dependency, Workload: snapshot.workload, Fault: snapshot.fault,
		Processes: snapshot.processes, Native: native,
		Input: protocol.EffectiveInput{
			AdapterID: adapterID, AdapterVersion: config.AdapterVersion, AgentBinarySHA256: clientHash,
			SystemID: systemID, Runtime: runtime.GOOS + "/" + runtime.GOARCH,
			AuthorityProtocol: protocol.AuthorityProtocol, DependencyProtocol: protocol.DependencyProtocol,
			FailureProtocol: protocol.FailureProtocol, OracleProtocol: protocol.OracleProtocol,
			DestinationID: snapshot.destination.DestinationID, OracleVisibility: protocol.OracleVisibility(),
			HostLimits: map[string]int64{"client_processes": 3, "cohort_size": 1},
			Settings:   settings,
		},
	})
}

func validateConfig(config Config) error {
	versionHash := strings.TrimPrefix(config.AdapterVersion, "source-sha256:")
	if config.Root == "" || (config.Probe != protocol.ProbeUnsafe && config.Probe != protocol.ProbeProtected) || config.Trial < 1 ||
		config.ClientBinary == "" || config.AdapterVersion == versionHash || len(versionHash) != 64 {
		return fmt.Errorf("%w: ABA live config requires root, faulted probe, trial, client, and source hash", protocol.ErrInvalidEvidence)
	}
	if _, err := hex.DecodeString(versionHash); err != nil {
		return fmt.Errorf("%w: ABA adapter source hash: %v", protocol.ErrInvalidEvidence, err)
	}
	info, err := os.Stat(config.ClientBinary)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("%w: ABA client must be an executable regular file", protocol.ErrInvalidEvidence)
	}
	return nil
}

func randomRunID(probe protocol.Probe, trial int) (string, error) {
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate ABA run identity: %w", err)
	}
	return fmt.Sprintf("aba-live-%s-trial-%d-%s", probe, trial, hex.EncodeToString(entropy[:])), nil
}

func launchRequest(endpoint string, record *recorder, owner string, generation uint64, capability, attemptID, parentAttemptID string,
	retryOrdinal int, retryCause, workerID string, actions []Action,
) LaunchRequest {
	return LaunchRequest{
		Endpoint: endpoint, RunID: record.runID, LogicalOperationID: record.operationID, WorkItemID: record.workItemID,
		OwnerID: owner, Generation: generation, Capability: capability, AttemptID: attemptID, ParentAttemptID: parentAttemptID,
		RetryOrdinal: retryOrdinal, RetryCause: retryCause, WorkerID: workerID, Actions: actions,
	}
}

func startClient(ctx context.Context, binary, privateDir string, request LaunchRequest) (*runningClient, error) {
	data, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode %s launch request: %w", request.AttemptID, err)
	}
	requestFile := filepath.Join(privateDir, request.AttemptID+".json")
	file, err := os.OpenFile(requestFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create %s launch request: %w", request.AttemptID, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("write %s launch request: %w", request.AttemptID, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("sync %s launch request: %w", request.AttemptID, err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close %s launch request: %w", request.AttemptID, err)
	}

	client := &runningClient{done: make(chan error, 1)}
	client.command = exec.CommandContext(ctx, binary, "--request", requestFile)
	client.command.Stdout = &client.stdout
	client.command.Stderr = &client.stderr
	if err := client.command.Start(); err != nil {
		return nil, fmt.Errorf("start %s client: %w", request.AttemptID, err)
	}
	startIdentity, err := agentprocess.ProcessStartIdentity(client.command.Process.Pid)
	if err != nil {
		_ = client.command.Process.Kill()
		_ = client.command.Wait()
		return nil, fmt.Errorf("capture %s process identity: %w", request.AttemptID, err)
	}
	client.processIdentity = fmt.Sprintf("pid:%d:start:%s", client.command.Process.Pid, startIdentity)
	go func() { client.done <- client.command.Wait() }()
	return client, nil
}

func (c *runningClient) wait(ctx context.Context) (ClientResult, error) {
	c.waitOnce.Do(func() {
		select {
		case <-ctx.Done():
			c.waitErr = ctx.Err()
		case c.waitErr = <-c.done:
		}
		if c.waitErr != nil {
			c.waitErr = fmt.Errorf("process wait: %w; stderr=%s", c.waitErr, strings.TrimSpace(c.stderr.String()))
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(c.stdout.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&c.result); err != nil {
			c.waitErr = fmt.Errorf("decode client result: %w; stderr=%s", err, strings.TrimSpace(c.stderr.String()))
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			c.waitErr = errors.New("client result contains trailing data")
			return
		}
		if c.result.ProcessIdentity != c.processIdentity {
			c.waitErr = fmt.Errorf("client process identity %q differs from observed %q", c.result.ProcessIdentity, c.processIdentity)
		}
	})
	return c.result, c.waitErr
}

func requireDecisions(result ClientResult, accepted bool, actions ...Action) error {
	if len(result.Responses) != len(actions) {
		return fmt.Errorf("%w: %s generation %d returned %d responses, want %d", protocol.ErrInvalidEvidence,
			result.OwnerID, result.Generation, len(result.Responses), len(actions))
	}
	for index, action := range actions {
		if result.Responses[index].Action != action || result.Responses[index].Accepted != accepted {
			return fmt.Errorf("%w: %s generation %d action %s accepted=%t, want %t", protocol.ErrInvalidEvidence,
				result.OwnerID, result.Generation, result.Responses[index].Action, result.Responses[index].Accepted, accepted)
		}
	}
	return nil
}
