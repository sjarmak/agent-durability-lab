package publication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

func TestEpisodePlansAreFrozenAndValidForEveryStratum(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases() {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			request := EpisodeRequest{Case: benchmarkCase, Probe: probe, Slot: 1}
			plan, err := BuildEpisodePlan(request)
			if err != nil {
				t.Fatalf("%s/%s: %v", benchmarkCase, probe, err)
			}
			if plan.Case != benchmarkCase || plan.Probe != probe || plan.Trial != 1 {
				t.Fatalf("%s/%s: %+v", benchmarkCase, probe, plan)
			}
		}
	}
}

func TestObservedRecoveryEpisodesDistinguishUnsafeAndPassProtected(t *testing.T) {
	for _, benchmarkCase := range protocol.Cases()[1:] {
		for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
			benchmarkCase, probe := benchmarkCase, probe
			t.Run(string(benchmarkCase)+"/"+string(probe), func(t *testing.T) {
				request := EpisodeRequest{
					Phase: PhasePilot, PairID: fmt.Sprintf("test/%s/%s", benchmarkCase, probe), PairIndex: 1,
					Case: benchmarkCase, Probe: probe, Slot: 1,
				}
				plan, err := BuildEpisodePlan(request)
				if err != nil {
					t.Fatal(err)
				}
				clock := newSteppingClock(2 * time.Millisecond)
				timing := NewTimingRecorder(clock, SystemTemporal, request)
				episode, err := NewEpisodeRuntime(EpisodeRuntimeConfig{
					Request: request, Plan: plan, SystemID: SystemTemporal, AdapterID: "fake-observed-v1",
					AdapterVersion: "source-sha256:" + digest("fake-adapter"), AgentBinarySHA256: digest("fake-agent"),
					Clock: clock, Timing: timing,
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := runFakeRounds(context.Background(), plan, episode); err != nil {
					t.Fatal(err)
				}
				result, err := episode.Finish(context.Background(), t.TempDir(), []protocol.NativeRecord{
					nativeRecord(1, "fake_durable_episode", string(benchmarkCase)+"/"+string(probe), time.Now()),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := result.Verdict.Validate(); err != nil {
					t.Fatal(err)
				}
				if result.Verdict.Admission != protocol.AdmissionValid || result.Verdict.Diagnosability != protocol.OutcomePass {
					t.Fatalf("verdict = %+v", result.Verdict)
				}
				if probe == protocol.ProbeUnsafe {
					if result.Verdict.Correctness == protocol.OutcomePass && result.Verdict.Safety == protocol.OutcomePass && result.Verdict.Liveness == protocol.OutcomePass {
						t.Fatalf("unsafe control did not distinguish: %+v", result.Verdict)
					}
				} else if result.Verdict.Correctness != protocol.OutcomePass || result.Verdict.Safety != protocol.OutcomePass ||
					result.Verdict.Liveness != protocol.OutcomePass || !result.Verdict.EfficiencyEligible {
					t.Fatalf("parity failed: %+v", result.Verdict)
				}
				if err := ValidateTimingEvents(probe, timing.Events()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func newSteppingClock(step time.Duration) *steppingClock {
	return &steppingClock{now: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC), step: step}
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now
	c.now = c.now.Add(c.step)
	return now
}

func TestObservedABAEpisodesRejectGenerationSevenAfterOwnerReturnsToA(t *testing.T) {
	for _, probe := range []protocol.Probe{protocol.ProbeUnfaulted, protocol.ProbeUnsafe, protocol.ProbeProtected} {
		probe := probe
		t.Run(string(probe), func(t *testing.T) {
			request := EpisodeRequest{
				Phase: PhasePilot, PairID: "test/aba/" + string(probe), PairIndex: 1,
				Case: protocol.CaseABAReacquisition, Probe: probe, Slot: 1,
			}
			plan, err := BuildEpisodePlan(request)
			if err != nil {
				t.Fatal(err)
			}
			timing := NewTimingRecorder(wallClock{}, SystemTemporal, request)
			episode, err := NewEpisodeRuntime(EpisodeRuntimeConfig{
				Request: request, Plan: plan, SystemID: SystemTemporal, AdapterID: "fake-observed-v1",
				AdapterVersion: "source-sha256:" + digest("fake-adapter"), AgentBinarySHA256: digest("fake-agent"),
				Clock: wallClock{}, Timing: timing,
			})
			if err != nil {
				t.Fatal(err)
			}
			if probe == protocol.ProbeUnfaulted {
				if err := runFakeRounds(context.Background(), plan, episode); err != nil {
					t.Fatal(err)
				}
			} else if err := runFakeABA(context.Background(), episode, probe == protocol.ProbeUnsafe); err != nil {
				t.Fatal(err)
			}
			result, err := episode.Finish(context.Background(), t.TempDir(), []protocol.NativeRecord{
				nativeRecord(1, "fake_aba_journal", string(probe), time.Now()),
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Verdict.Admission != protocol.AdmissionValid || result.Verdict.Diagnosability != protocol.OutcomePass {
				t.Fatalf("verdict = %+v", result.Verdict)
			}
			if probe == protocol.ProbeUnsafe {
				if result.Verdict.Safety != protocol.OutcomeFail || result.Verdict.Liveness != protocol.OutcomeFail || result.Verdict.Metrics.StaleActionAcceptCount == 0 {
					t.Fatalf("unsafe verdict = %+v", result.Verdict)
				}
			} else if result.Verdict.Correctness != protocol.OutcomePass || result.Verdict.Safety != protocol.OutcomePass ||
				result.Verdict.Liveness != protocol.OutcomePass || !result.Verdict.EfficiencyEligible {
				t.Fatalf("protected/unfaulted verdict = %+v", result.Verdict)
			}
			if err := ValidateTimingEvents(probe, timing.Events()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runFakeABA(ctx context.Context, episode *EpisodeRuntime, staleAccepted bool) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- episode.BeginABA(ctx, NativeIdentity{WorkerID: "fake-worker-g7", ProcessIdentity: "pid:fake-g7"})
	}()
	if err := episode.WaitABABarrier(ctx); err != nil {
		return err
	}
	if err := episode.AdvanceABA(8, NativeIdentity{WorkerID: "fake-worker-g8", ProcessIdentity: "pid:fake-g8"}); err != nil {
		return err
	}
	if err := episode.AdvanceABA(9, NativeIdentity{WorkerID: "fake-worker-g9", ProcessIdentity: "pid:fake-g9"}); err != nil {
		return err
	}
	if err := episode.CompleteABACurrent(ctx); err != nil {
		return err
	}
	episode.ReleaseABA(staleAccepted)
	return <-errCh
}

func runFakeRounds(ctx context.Context, plan EpisodePlan, episode *EpisodeRuntime) error {
	for _, round := range plan.Rounds {
		gate := make(chan struct{})
		capacity := make(chan struct{}, 8)
		errorsByWork := make(chan error, len(round.Work))
		var wait sync.WaitGroup
		for index, work := range round.Work {
			index, work := index, work
			wait.Add(1)
			go func() {
				defer wait.Done()
				select {
				case <-ctx.Done():
					errorsByWork <- ctx.Err()
					return
				case <-gate:
				}
				if work.DelayMillis > 0 {
					timer := time.NewTimer(time.Duration(work.DelayMillis) * time.Millisecond)
					select {
					case <-ctx.Done():
						if !timer.Stop() {
							<-timer.C
						}
						errorsByWork <- ctx.Err()
						return
					case <-timer.C:
					}
				}
				select {
				case <-ctx.Done():
					errorsByWork <- ctx.Err()
					return
				case capacity <- struct{}{}:
				}
				defer func() { <-capacity }()
				errorsByWork <- episode.RunWork(ctx, work, NativeIdentity{
					WorkerID: fmt.Sprintf("fake-worker-%d", index%8+1), ProcessIdentity: fmt.Sprintf("pid:fake-%d", index%8+1),
				})
			}()
		}
		close(gate)
		wait.Wait()
		close(errorsByWork)
		for err := range errorsByWork {
			if err != nil {
				return err
			}
		}
	}
	return nil
}
