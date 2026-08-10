package agentprocess

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/sjarmak/temporal_projects/internal/agentsim"
)

type LaunchRequest struct {
	StorePath  string          `json:"store_path"`
	BarrierURL string          `json:"barrier_url"`
	Config     agentsim.Config `json:"config"`
}

type Process struct {
	PID            int    `json:"pid"`
	StartIdentity  string `json:"start_identity"`
	ProcessGroupID int    `json:"process_group_id,omitempty"`
	RequestPath    string `json:"request_path"`
	LogPath        string `json:"log_path"`
}

type Launcher struct {
	binary string
	runDir string
}

func NewLauncher(binary, runDir string) *Launcher {
	return &Launcher{binary: binary, runDir: runDir}
}

func (l *Launcher) Launch(request LaunchRequest) (Process, error) {
	if err := l.validate(request); err != nil {
		return Process{}, err
	}
	if err := os.MkdirAll(l.runDir, 0o750); err != nil {
		return Process{}, fmt.Errorf("create agent run directory: %w", err)
	}
	requestPath, err := l.writeRequest(request)
	if err != nil {
		return Process{}, err
	}
	requestPublished := true
	defer func() {
		if !requestPublished {
			_ = os.Remove(requestPath)
		}
	}()

	logFile, err := os.CreateTemp(l.runDir, fmt.Sprintf("agent-g%d-*.log", request.Config.Lease.Generation))
	if err != nil {
		requestPublished = false
		return Process{}, fmt.Errorf("create agent log: %w", err)
	}
	logPath := logFile.Name()
	if err := logFile.Chmod(0o600); err != nil {
		_ = logFile.Close()
		requestPublished = false
		return Process{}, fmt.Errorf("set agent log permissions: %w", err)
	}

	command := exec.Command(l.binary, "--request", requestPath)
	command.Stdin = nil
	command.Stdout = logFile
	command.Stderr = logFile
	// A detached process can outlive and be fenced from its Worker; inheriting the
	// Worker's ambient credentials would preserve authority outside that fence.
	command.Env = []string{}
	configureDetached(command)
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		requestPublished = false
		return Process{}, fmt.Errorf("start agent process: %w", err)
	}
	pid := command.Process.Pid
	startIdentity, identityErr := ProcessStartIdentity(pid)
	processGroupID, groupErr := ProcessGroupID(pid)
	closeErr := logFile.Close()
	if identityErr != nil || groupErr != nil || closeErr != nil {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		requestPublished = false
		return Process{}, errors.Join(identityErr, groupErr, closeErr)
	}
	// Waiting in a detached goroutine does not couple the child lifetime to the
	// caller or Worker. It only lets this still-live launcher reap the child;
	// Process.Release would leave one zombie per completed agent until the
	// benchmark process itself exited.
	go func() { _ = command.Wait() }()
	return Process{
		PID: pid, StartIdentity: startIdentity, ProcessGroupID: processGroupID,
		RequestPath: requestPath, LogPath: logPath,
	}, nil
}

func (l *Launcher) validate(request LaunchRequest) error {
	if l.binary == "" || l.runDir == "" || request.StorePath == "" || request.BarrierURL == "" {
		return errors.New("launch requires binary, run directory, store path, and barrier URL")
	}
	info, err := os.Stat(l.binary)
	if err != nil {
		return fmt.Errorf("inspect agent binary: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("agent binary %q is not executable", l.binary)
	}
	if request.Config.Lease.SessionID == "" || request.Config.Lease.Generation == 0 || request.Config.Lease.OwnerToken == "" {
		return errors.New("launch requires a complete session lease")
	}
	return nil
}

func (l *Launcher) writeRequest(request LaunchRequest) (string, error) {
	file, err := os.CreateTemp(l.runDir, fmt.Sprintf("agent-g%d-*.json", request.Config.Lease.Generation))
	if err != nil {
		return "", fmt.Errorf("create agent request: %w", err)
	}
	path := file.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(path)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("set agent request permissions: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(request); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("encode agent request: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("sync agent request: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close agent request: %w", err)
	}
	committed = true
	return path, nil
}
