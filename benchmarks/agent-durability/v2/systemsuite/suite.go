package systemsuite

import (
	"context"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/abalive"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/calibration"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

type Executor interface {
	Execute(context.Context, systemplan.Plan) (systemplan.Execution, error)
}

type Config struct {
	Root         string
	Trials       int
	ClientBinary string
}

func Run(ctx context.Context, executor Executor, config Config) ([]protocol.Verdict, error) {
	if executor == nil || config.Root == "" || config.Trials < 1 || config.ClientBinary == "" {
		return nil, fmt.Errorf("%w: executor, root, trials, and common client are required", protocol.ErrInvalidEvidence)
	}
	agentHash, err := protocol.FileSHA256(config.ClientBinary)
	if err != nil {
		return nil, err
	}
	var verdicts []protocol.Verdict
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			for trial := 1; trial <= config.Trials; trial++ {
				plan, err := systemplan.Build(benchmarkCase, probe, trial)
				if err != nil {
					return nil, err
				}
				execution, err := executor.Execute(ctx, plan)
				if err != nil {
					return nil, fmt.Errorf("execute %s %s trial %d: %w", benchmarkCase, probe, trial, err)
				}
				runDir, err := writeCommonEvidence(ctx, config, plan, execution, agentHash)
				if err != nil {
					return nil, err
				}
				verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
				if err != nil {
					return nil, err
				}
				if err := expectedVerdict(verdict); err != nil {
					return nil, err
				}
				verdicts = append(verdicts, verdict)
			}
		}
	}
	return verdicts, nil
}

func writeCommonEvidence(ctx context.Context, config Config, plan systemplan.Plan, execution systemplan.Execution, agentHash string) (string, error) {
	settings := make(map[string]string, len(execution.Settings)+1)
	for name, value := range execution.Settings {
		settings[name] = value
	}
	settings["system_execution_id"] = execution.ExecutionID
	if plan.Case == protocol.CaseABAReacquisition && plan.Probe != protocol.ProbeUnfaulted {
		return abalive.Run(ctx, abalive.Config{
			Root: config.Root, Probe: plan.Probe, Trial: plan.Trial, ClientBinary: config.ClientBinary,
			AdapterID: execution.AdapterID, AdapterVersion: execution.AdapterVersion, SystemID: execution.SystemID,
			Native: execution.Native, Settings: settings,
		})
	}
	return calibration.Run(ctx, calibration.Config{
		Root: config.Root, Case: plan.Case, Probe: plan.Probe, Trial: plan.Trial,
		AdapterID: execution.AdapterID, AdapterVersion: execution.AdapterVersion, AgentBinarySHA256: agentHash,
		SystemID: execution.SystemID, Native: execution.Native, Settings: settings,
	})
}

func expectedVerdict(verdict protocol.Verdict) error {
	if verdict.Admission != protocol.AdmissionValid || verdict.Diagnosability != protocol.OutcomePass {
		return fmt.Errorf("%w: system evidence was not admitted: %+v", protocol.ErrInvalidEvidence, verdict)
	}
	if verdict.Probe == protocol.ProbeUnsafe {
		if verdict.Correctness == protocol.OutcomePass && verdict.Safety == protocol.OutcomePass && verdict.Liveness == protocol.OutcomePass {
			return fmt.Errorf("%w: unsafe control did not distinguish: %+v", protocol.ErrInvalidEvidence, verdict)
		}
		return nil
	}
	if verdict.Correctness != protocol.OutcomePass || verdict.Safety != protocol.OutcomePass || verdict.Liveness != protocol.OutcomePass {
		return fmt.Errorf("%w: protected or unfaulted system run failed: %+v", protocol.ErrInvalidEvidence, verdict)
	}
	return nil
}
