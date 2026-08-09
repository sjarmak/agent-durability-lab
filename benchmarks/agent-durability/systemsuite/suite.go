package systemsuite

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/livecommon"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/oracle"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/protocol"
	v2protocol "github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

type Executor interface {
	Execute(context.Context, systemplan.Plan) (systemplan.Execution, error)
}

type Config struct {
	Root        string
	Trials      int
	AgentBinary string
}

func Run(ctx context.Context, executor Executor, config Config) ([]protocol.Verdict, error) {
	if executor == nil || config.Root == "" || config.Trials < 1 || config.AgentBinary == "" {
		return nil, fmt.Errorf("%w: executor, root, trials, and common agent are required", protocol.ErrInvalidEvidence)
	}
	var verdicts []protocol.Verdict
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			for trial := 1; trial <= config.Trials; trial++ {
				plan, err := systemPlan(benchmarkCase, probe, trial)
				if err != nil {
					return nil, err
				}
				execution, err := executor.Execute(ctx, plan)
				if err != nil {
					return nil, err
				}
				runDir, err := livecommon.Run(ctx, livecommon.Config{
					Root: config.Root, Case: benchmarkCase, Probe: probe, Trial: trial, AgentBinary: config.AgentBinary,
					AdapterID: execution.AdapterID + "-contract-v1", AdapterVersion: execution.AdapterVersion,
					SystemID: execution.SystemID, Native: nativeV1(execution.Native), Settings: mergedSettings(execution),
				})
				if err != nil {
					return nil, err
				}
				verdict, err := oracle.EvaluateAndWrite(ctx, runDir)
				if err != nil {
					return nil, err
				}
				want := protocol.VerdictValidPass
				if probe == protocol.ProbeUnsafe {
					want = protocol.VerdictValidFail
				}
				if verdict.Class != want {
					return nil, fmt.Errorf("%w: v1 system verdict %s/%s = %+v", protocol.ErrInvalidEvidence, benchmarkCase, probe, verdict)
				}
				verdicts = append(verdicts, verdict)
			}
		}
	}
	return verdicts, nil
}

func systemPlan(benchmarkCase protocol.CaseID, probe protocol.Probe, trial int) (systemplan.Plan, error) {
	mapped := map[protocol.CaseID]v2protocol.CaseID{
		protocol.CaseSurvivingExecutor:       v2protocol.CaseSilentProgress,
		protocol.CaseAmbiguousEffect:         v2protocol.CaseLayeredRetryAmplification,
		protocol.CaseStaleGeneration:         v2protocol.CaseABAReacquisition,
		protocol.CaseCancellationUnreachable: v2protocol.CaseSilentProgress,
	}[benchmarkCase]
	return systemplan.Build(mapped, v2protocol.Probe(probe), trial)
}

func nativeV1(records []v2protocol.NativeRecord) []protocol.NativeRecord {
	result := make([]protocol.NativeRecord, 0, len(records))
	for _, record := range records {
		detail, _ := json.Marshal(map[string]string{"time": record.Time, "detail": record.Detail})
		result = append(result, protocol.NativeRecord{
			Sequence: uint64(len(result) + 1), Kind: record.Kind, Detail: string(detail),
		})
	}
	return result
}

func mergedSettings(execution systemplan.Execution) map[string]string {
	settings := make(map[string]string, len(execution.Settings)+1)
	for name, value := range execution.Settings {
		settings[name] = value
	}
	settings["system_execution_id"] = execution.ExecutionID
	return settings
}
