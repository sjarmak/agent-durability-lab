package lab

import (
	"errors"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

type WorkerConfig struct {
	Command         ClaudeCommand
	LauncherBinary  string
	FaultBoundary   FaultBoundary
	EffectBinary    string
	DestinationPath string
	WorkspacePath   string
	EffectPayload   string
	BarrierURL      string
	BarrierPoint    string
	RunRoot         string
	WorkerID        string
}

type WorkerRegistrar interface {
	RegisterWorkflowWithOptions(any, workflow.RegisterOptions)
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}

func RegisterWorker(registrar WorkerRegistrar, config WorkerConfig) error {
	if registrar == nil || !config.valid() {
		return errors.New("worker registration requires a registrar and complete Claude experiment configuration")
	}
	registrar.RegisterWorkflowWithOptions(
		DirectClaudeWorkflow,
		workflow.RegisterOptions{Name: DirectClaudeWorkflowName},
	)
	registrar.RegisterActivityWithOptions(
		Activities{
			Command: config.Command, LauncherBinary: config.LauncherBinary, FaultBoundary: config.FaultBoundary,
			EffectBinary:    config.EffectBinary,
			DestinationPath: config.DestinationPath, WorkspacePath: config.WorkspacePath,
			EffectPayload: config.EffectPayload, BarrierURL: config.BarrierURL,
			BarrierPoint: config.BarrierPoint, RunRoot: config.RunRoot, WorkerID: config.WorkerID,
		}.RunClaude,
		activity.RegisterOptions{Name: RunClaudeActivityName},
	)
	return nil
}

func (c WorkerConfig) valid() bool {
	return c.Command.Binary != "" && c.LauncherBinary != "" && c.FaultBoundary.valid() &&
		c.Command.WorkDir != "" && c.Command.Model != "" &&
		c.Command.MaxBudgetUSD != "" && c.Command.MaxTurns > 0 && c.EffectBinary != "" &&
		c.DestinationPath != "" && c.WorkspacePath != "" && c.EffectPayload != "" &&
		c.BarrierURL != "" && c.BarrierPoint != "" && c.RunRoot != "" && c.WorkerID != ""
}
