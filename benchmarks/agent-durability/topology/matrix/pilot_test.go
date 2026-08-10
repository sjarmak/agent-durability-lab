package matrix

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/internal/testfixture"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/runner"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/semantics"
)

func TestAuditPilotScheduleHasFrozenGlobalBalance(t *testing.T) {
	registration := loadPilotRegistration(t)
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePilot)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := auditPilotSchedule(registration, schedule)
	if err != nil {
		t.Fatal(err)
	}
	want := PilotScheduleAudit{
		Strata: 88, Pairs: 264, ArmExecutions: 528, DirectFirst: 132,
		ChildFirst: 132, PairsPerStratum: 3,
	}
	if !reflect.DeepEqual(audit, want) {
		t.Fatalf("pilot audit = %+v, want %+v", audit, want)
	}

	drifted := schedule.Clone()
	drifted.Blocks[0].TopologyOrder[0], drifted.Blocks[0].TopologyOrder[1] =
		drifted.Blocks[0].TopologyOrder[1], drifted.Blocks[0].TopologyOrder[0]
	if _, err := auditPilotSchedule(registration, drifted); err == nil {
		t.Fatal("pilot schedule order drift was admitted")
	}
}

func TestAccountPilotPairSeparatesUnsafeDistinctionFromPassingOutcomes(t *testing.T) {
	unsafe := runner.PairExecution{
		Admission: protocol.AdmissionValid,
		Block:     protocol.PairBlock{Stratum: protocol.Stratum{Probe: protocol.ProbeUnsafe}},
		Arms: []runner.ArmRun{
			{Verdict: validUnsafeVerdict()},
			{Verdict: validUnsafeVerdict()},
		},
	}
	passing := runner.PairExecution{
		Admission: protocol.AdmissionValid,
		Block:     protocol.PairBlock{Stratum: protocol.Stratum{Probe: protocol.ProbeProtected}},
		Arms: []runner.ArmRun{
			{Verdict: validPassingVerdict()},
			{Verdict: validPassingVerdict()},
		},
	}
	var report PilotReport
	if err := accountPilotPair(&report, unsafe); err != nil {
		t.Fatal(err)
	}
	if err := accountPilotPair(&report, passing); err != nil {
		t.Fatal(err)
	}
	if report.AttemptedPairs != 2 || report.ValidPairs != 2 || report.ValidArms != 4 ||
		report.UnsafeArms != 2 || report.UnsafeArmsDistinguished != 2 ||
		report.ProtectedOrUnfaultedArms != 2 || report.ProtectedOrUnfaultedArmsPassed != 2 ||
		report.HistoriesReplayed != 4 {
		t.Fatalf("pilot report = %+v", report)
	}

	unsafe.Arms[0].Verdict.Safety = protocol.OutcomePass
	if err := accountPilotPair(&PilotReport{}, unsafe); err == nil {
		t.Fatal("non-distinguishing unsafe pair was accepted")
	}
}

func TestAuditPilotArmRejectsFixtureHistoryInProduction(t *testing.T) {
	registration := loadPilotRegistration(t)
	schedule, err := protocol.BuildSchedule(registration, protocol.PhasePilot)
	if err != nil {
		t.Fatal(err)
	}
	block := schedule.Blocks[0]
	bundle := testfixture.Bundle(block, block.TopologyOrder[0])
	arm := runner.ArmRun{Topology: block.TopologyOrder[0], Order: 1, RunID: bundle.Manifest.RunID, Verdict: bundle.Verdict}
	if err := auditPilotArm(block, arm, bundle, true); err != nil {
		t.Fatalf("fixture-control audit: %v", err)
	}
	if err := auditPilotArm(block, arm, bundle, false); err == nil {
		t.Fatal("production pilot admitted fixture native history")
	}
}

func TestSourceSetHashBindsPathsBytesAndMembership(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "b.go"), []byte("package nested\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "prior-evidence.json"), []byte(`{"result":"excluded"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := collectSourceSet(root, []string{"a.go", "nested"})
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Artifacts) != 2 || set.SHA256 == "" {
		t.Fatalf("source set = %+v", set)
	}
	if err := validateSourceSet(set); err != nil {
		t.Fatal(err)
	}
	set.Artifacts[0].Bytes++
	if err := validateSourceSet(set); err == nil {
		t.Fatal("mutated source artifact was admitted")
	}
}

func TestPilotFreezeManifestBindsRequiredEnvironment(t *testing.T) {
	set := SourceSet{SHA256: testfixture.Hash('a'), Artifacts: []SourceArtifact{{
		Path: "go.mod", SHA256: testfixture.Hash('b'), Bytes: 10,
	}}}
	set.SHA256, _ = sourceSetDigest(set.Artifacts)
	manifest := PilotFreezeManifest{
		ProtocolVersion: protocol.PublicationProtocolVersion, Kind: PilotFreezeKind,
		CreatedAtUTC: "2026-08-10T04:00:00Z", TrackerBeadID: PilotTrackerBeadID,
		ReviewDisposition: "qualified", PilotRoot: "pilot-20260810-v1", PilotQualified: true, PublicationExcluded: true,
		RunnerSource: set, AnalyzerSource: set, AgentProcessSource: set, DestinationSource: set, BarrierSource: set,
		RunnerBinarySHA256: testfixture.Hash('c'), AnalyzerBinarySHA256: testfixture.Hash('c'),
		AgentBinarySHA256: testfixture.Hash('d'), TemporalBinarySHA256: testfixture.Hash('e'),
		ContractSHA256: testfixture.Hash('f'), PreregistrationSHA256: testfixture.Hash('1'), ScheduleSHA256: testfixture.Hash('2'),
		Versions: VersionEnvelope{TemporalServer: "1.31.2", TemporalCLI: "1.8.0", TemporalGoSDK: "1.47.0", Go: "go1.25.0"},
		Host:     HostEnvelope{GOOS: "linux", GOARCH: "amd64", HostnameSHA256: testfixture.Hash('3'), CPUCount: 8},
		Worker:   semantics.FrozenWorkerConfiguration(), PublicationSeed: 1618033988, PilotSeed: 1414213562,
		Pilot: PilotFreezeSummary{Pairs: 264, Arms: 528, HistoriesReplayed: 528},
	}
	if err := validatePilotFreeze(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.PilotQualified = false
	if err := validatePilotFreeze(manifest); err == nil {
		t.Fatal("unqualified publication harness freeze was admitted")
	}
}

func TestBuildPilotFreezeBindsQualifiedReport(t *testing.T) {
	set := SourceSet{Artifacts: []SourceArtifact{{
		Path: "go.mod", SHA256: testfixture.Hash('b'), Bytes: 10,
	}}}
	set.SHA256, _ = sourceSetDigest(set.Artifacts)
	report := PilotReport{
		Qualified: true, ValidPairs: 264, ValidArms: 528, HistoriesReplayed: 528,
		HarnessBinarySHA256: testfixture.Hash('c'), AgentBinarySHA256: testfixture.Hash('d'),
		TemporalBinarySHA256: testfixture.Hash('e'), ContractSHA256: testfixture.Hash('f'),
		PreregistrationSHA256: testfixture.Hash('1'), ScheduleSHA256: testfixture.Hash('2'),
	}
	registration := protocol.Preregistration{Population: protocol.PopulationPolicy{
		PublicationSeed: 1618033988, PilotSeed: 1414213562,
	}}
	versions := VersionEnvelope{
		TemporalServer: "1.31.2", TemporalCLI: "1.8.0", TemporalGoSDK: "1.47.0", Go: "go1.25.0",
	}
	manifest, err := buildPilotFreeze(PilotConfig{}, filepath.Join(t.TempDir(), "pilot-v6"), report, registration, pilotSources{
		runner: set, analyzer: set, agent: set, destination: set, barrier: set,
	}, versions)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.PilotRoot != "pilot-v6" || manifest.RunnerBinarySHA256 != report.HarnessBinarySHA256 ||
		manifest.AgentBinarySHA256 != report.AgentBinarySHA256 || manifest.TemporalBinarySHA256 != report.TemporalBinarySHA256 ||
		manifest.PublicationSeed != registration.Population.PublicationSeed || manifest.PilotSeed != registration.Population.PilotSeed ||
		manifest.Pilot != (PilotFreezeSummary{Pairs: 264, Arms: 528, HistoriesReplayed: 528}) {
		t.Fatalf("pilot freeze = %+v", manifest)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	missingVersion, err := dependencyVersion(executable, "example.invalid/missing-dependency")
	if err == nil || missingVersion != "" {
		t.Fatalf("missing dependency version = %q, %v", missingVersion, err)
	}
}

func TestFinalizePilotQualificationRequiresEveryScheduledArmAndHistory(t *testing.T) {
	report := PilotReport{
		Schedule:       PilotScheduleAudit{Pairs: 264, ArmExecutions: 528},
		AttemptedPairs: 264, ValidPairs: 264, AttemptedArms: 528, ValidArms: 528,
		UnsafeArms: 114, UnsafeArmsDistinguished: 114,
		ProtectedOrUnfaultedArms: 414, ProtectedOrUnfaultedArmsPassed: 414,
		HistoriesReplayed: 528,
	}
	finalizePilotQualification(&report)
	if !report.Qualified {
		t.Fatalf("complete pilot did not qualify: %+v", report)
	}
	report.HistoriesReplayed--
	finalizePilotQualification(&report)
	if report.Qualified {
		t.Fatal("pilot with a missing replay qualified")
	}
}

func TestPilotPreflightFailureDoesNotCreateEvidenceRoot(t *testing.T) {
	temporary := t.TempDir()
	registration := filepath.Join(temporary, "invalid-preregistration.json")
	contract := filepath.Join(temporary, "contract.json")
	if err := os.WriteFile(registration, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contract, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(temporary, "pilot-root")
	_, err = RunPilot(context.Background(), PilotConfig{
		Root: root, PreregistrationPath: registration, ContractPath: contract,
		TemporalPath: executable, WorkRoot: filepath.Join(temporary, "work"),
		AgentBinary: executable, SourceRoot: temporary,
	})
	if err == nil {
		t.Fatal("invalid preregistration was accepted")
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("preflight created evidence root: %v", statErr)
	}
}

func TestCollectPilotSourcesCoversFrozenComponents(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	sources, err := collectPilotSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for name, set := range map[string]SourceSet{
		"runner": sources.runner, "analyzer": sources.analyzer, "agent": sources.agent,
		"destination": sources.destination, "barrier": sources.barrier,
	} {
		if err := validateSourceSet(set); err != nil {
			t.Fatalf("%s source set: %v", name, err)
		}
	}
}

func TestAuditRejectedPilotEvidenceReconstructsEveryPair(t *testing.T) {
	root := os.Getenv("TOPOLOGY_PILOT_AUDIT_ROOT")
	if root == "" {
		t.Skip("set TOPOLOGY_PILOT_AUDIT_ROOT to preserved pilot evidence")
	}
	report, err := AuditPilot(root)
	if err != nil {
		t.Fatal(err)
	}
	if report.AttemptedPairs != 264 || report.ValidPairs != 260 || report.InvalidPairs != 4 ||
		report.AttemptedArms != 528 || report.ValidArms != 520 || report.HistoriesReplayed != 520 || report.Qualified {
		t.Fatalf("rejected pilot audit = %+v", report)
	}
}

func validUnsafeVerdict() protocol.Verdict {
	return protocol.Verdict{
		Admission: protocol.AdmissionValid, Correctness: protocol.OutcomeFail, Safety: protocol.OutcomeFail,
		Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass,
	}
}

func validPassingVerdict() protocol.Verdict {
	return protocol.Verdict{
		Admission: protocol.AdmissionValid, Correctness: protocol.OutcomePass, Safety: protocol.OutcomePass,
		Liveness: protocol.OutcomePass, Diagnosability: protocol.OutcomePass, EfficiencyEligible: true,
	}
}

func loadPilotRegistration(t *testing.T) protocol.Preregistration {
	t.Helper()
	path := filepath.Join("..", "..", "topology-preregistration-v1.json")
	registration, err := protocol.LoadPreregistration(path)
	if err != nil {
		t.Fatal(err)
	}
	return registration
}
