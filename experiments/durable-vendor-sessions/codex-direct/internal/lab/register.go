package lab

import (
	"errors"

	"github.com/sjarmak/temporal_projects/internal/failureinject"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type WorkerConfig struct {
	Command           CodexCommand
	LauncherBinary    string
	FaultBoundary     FaultBoundary
	EffectBinary      string
	DestinationPath   string
	WorkspacePath     string
	EffectPayload     string
	BarrierURL        string
	BarrierCredential failureinject.Credential
	BarrierDirectory  string
	BarrierPoint      string
	RunRoot           string
	WorkerID          string
	SupervisorURL     string
	Hermetic          bool
}

type WorkerRegistrar interface {
	RegisterWorkflowWithOptions(any, workflow.RegisterOptions)
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}

func RegisterWorker(registrar WorkerRegistrar, config WorkerConfig) error {
	if registrar == nil || !config.valid() {
		return errors.New("worker registration requires complete Codex experiment configuration")
	}
	registrar.RegisterWorkflowWithOptions(CodexWorkflow, workflow.RegisterOptions{Name: CodexWorkflowName})
	registrar.RegisterActivityWithOptions(Activities{
		Command: config.Command, LauncherBinary: config.LauncherBinary, FaultBoundary: config.FaultBoundary,
		EffectBinary: config.EffectBinary, DestinationPath: config.DestinationPath,
		WorkspacePath: config.WorkspacePath, EffectPayload: config.EffectPayload,
		BarrierURL: config.BarrierURL, BarrierDirectory: config.BarrierDirectory, BarrierPoint: config.BarrierPoint,
		BarrierCredential: config.BarrierCredential,
		RunRoot:           config.RunRoot, WorkerID: config.WorkerID, SupervisorURL: config.SupervisorURL,
		Hermetic: config.Hermetic,
	}.RunCodex, activity.RegisterOptions{Name: RunCodexActivityName})
	return nil
}

func (c WorkerConfig) valid() bool {
	return c.Command.Binary != "" && c.Command.WorkDir != "" && c.Command.CodexHome != "" &&
		c.Command.Model != "" && c.Command.ReasoningEffort != "" && c.Command.OutputSchema != "" &&
		c.Command.Sandbox != "" && c.FaultBoundary.valid() && c.EffectBinary != "" &&
		c.DestinationPath != "" && c.WorkspacePath != "" && c.EffectPayload != "" &&
		c.BarrierURL != "" && (c.BarrierDirectory == "" || safeCommandPath(c.BarrierDirectory)) &&
		c.BarrierPoint != "" && c.RunRoot != "" && c.WorkerID != "" &&
		((c.FaultBoundary != FaultBeforeThreadObservation && c.FaultBoundary != FaultProcessFailureReplacement) ||
			c.LauncherBinary != "")
}
