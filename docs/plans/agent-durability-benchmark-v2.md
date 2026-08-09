# Agent durability benchmark v2: authority and recovery dynamics

**Status:** publication population and uncertainty report complete, 2026-08-09  
**Tracking:** `temporal_projects-zfz`  
**Contract:** [`contract-v2.json`](../../benchmarks/agent-durability/contract-v2.json)

## Decision this plan supports

Contract v1 asks whether one recovered logical operation remains correct at four
specific process, effect, ownership, and cancellation boundaries. V2 asks two
additional questions: whether authority survives an ABA owner-label cycle, and
whether recovery remains safe and live when retries or accumulated work compete
for finite capacity.

V2 does not replace, reinterpret, or expand a published v1 population. The v1
contract, protocol package, oracle, calibration, and evidence remain named by
`adl.cross-system.v1`. V2 uses a separate contract and protocol boundary named
`adl.cross-system.v2`.

The required v2 comparison is Temporal and the minimal PostgreSQL
queue/lease/outbox implementation. Restate and DBOS adoption are durable
follow-up work after their v1 adapters conform. The existing Claude and Codex
experiments are external-validity studies, not required v2 arms.

## Outcome model

A run first receives an admission result. An invalid run cannot make an
application claim. An admitted run receives four independent outcomes:

| Dimension | Question | Gate consequence |
| --- | --- | --- |
| Correctness | Does accepted final state match the immutable workload oracle? | Failure excludes efficiency comparison. |
| Safety | Did any prohibited stale, duplicate, destructive, or premature action occur? | Failure excludes efficiency comparison. |
| Liveness | Did eligible work reach the declared terminal state after recovery within the preregistered bound? | Failure excludes efficiency comparison. |
| Diagnosability | Can the independent evaluator reconstruct the causal path and classify known, failed, or unresolved state? | Failure excludes efficiency comparison. |

Latency, throughput, request count, durable growth, and cost are reported only
for matched populations that pass all four gates. The dimensions are never
combined into a scalar winner.

## Suite A: ABA authority

The authority case uses three immutable epochs under two recurring owner labels:

```text
A / generation 7 request blocks before authorization
B / generation 8 becomes current and completes its epoch
A / generation 9 becomes current
the delayed A / generation 7 request is released
```

The invariant is that generation 7 remains obsolete even though owner label A
is current again. Registration, destination mutation, completion,
acknowledgement, and stop all carry logical operation, generation, and opaque
capability. The unsafe control authorizes by owner label and must accept an old A
action. The protected arm validates the generation and capability atomically at
the accepting boundary. Any accepted generation-7 action, or a delayed stop that
harms generation 9, falsifies the protected claim.

The durability system supplies ordered durable procedure. The application
allocates authority epochs and capabilities. The destination supplies the final
stale-mutation rejection when it accepts effects.

## Suite B: recovery dynamics

| Profile | Exact boundary | Unsafe control | Protected mechanism | Primary decision |
| --- | --- | --- | --- | --- |
| Layered retry amplification | Scripted dependency failure activates after its first accepted request | Independent Workflow, Activity, client, and agent budgets multiply | One retry owner per boundary plus propagated attempt/time/cost budget | Are downstream attempts and cost bounded? |
| Outage, backlog, and herd recovery | Outage commits after steady state; restoration commits after the exact backlog target | Synchronized retries without jitter or global concurrency control | Seeded jitter, bounded retry concurrency, admission, and recovery budget | Does restoration drain rather than trigger a second outage? |
| Backpressure overload | A common gate releases a fixed or normalized 1x, 10x, or 100x cohort after readiness | Admit all work without queue or concurrency bounds | Bounded admission, explicit overload rejection, queue capacity, and worker concurrency | Does overload preserve accepted work and degrade predictably? |
| Poison-work isolation | Registered poison identities begin deterministic failure after mixed-cohort admission | Unbounded poison retry in the shared capacity pool | Attempt/cost budget, terminal quarantine, and fair or isolated capacity | Can healthy work progress beside permanent failures? |
| Silent progress | A registered executor emits one accepted progress event and then blocks while remaining externally alive | Process or Worker existence is treated as sufficient liveness | Durable progress deadline, authority revocation, then fenced replacement | Can a wedge be detected without revoking a legitimate wait? |

The common dependency is a hermetic state machine with exact outage,
restoration, response-script, and poison transitions plus an append-only physical
request journal. Wall-clock deadlines bound wedged runs; they do not select fault
windows.

## Causal evidence schema

Every causal event records a store-assigned sequence, UTC timestamp, unique event
ID, earlier causal parents, logical operation, optional work item, and the
identity of any attempt, authority, Worker, process, request, or effect involved.
Retry events additionally record retry layer, ordinal, cause, and parent attempt.
An attempt may have several lifecycle events, but its layer, ordinal, cause, and
parent cannot change.

The independent evaluator must be able to answer:

- which authority epoch was current for each accepted or rejected action;
- which delivery attempt and retry layer caused every physical dependency call;
- which work items were submitted, admitted, rejected, started, completed, and
  acknowledged;
- where progress stopped and which observation caused recovery;
- whether final state is supported, failed, or explicitly unresolved.

Missing or forward causal parents, identity drift, a moving UTC clock, mismatched
state inventories, and a fault outside its named event bracket invalidate the
run. An observed application invariant violation remains a valid failed run; it
is not mislabeled as malformed evidence.

## Latency and load definitions

| Metric | Common anchors |
| --- | --- |
| Queue / schedule-to-start | Item becomes eligible or admitted to its first authorized execution start |
| Execution | Cumulative authorized active intervals, reported separately from waits and backoff |
| Detection | Last expected accepted progress or failure transition to durable detection |
| Retry delay | Failed attempt completion to its causally linked successor start |
| Recovery | Fault or restoration commit to the next accepted forward progress and accepted outcome |
| End to end | Immutable submission to acknowledgement or declared terminal disposition |

Amplification is physical dependency requests divided by logical operations that
reached the dependency. Peak QPS uses a preregistered fixed window. Peak retry
concurrency comes from request intervals. Backlog depth is reconstructed exactly:
an outage-failed request adds one queued item and its matching successful retry
removes it; integration and restoration-relative completion times yield backlog
area and drain percentiles. Cost units are deterministic in the hermetic suite;
model tokens and provider charges belong only in separately labeled live vendor
validation.

Backpressure reports both a fixed absolute load under identical host limits and
a normalized load relative to each adapter's preregistered safe steady-state
rate. A normalized 10x result is not treated as an absolute throughput
comparison.

## Trial and publication policy

Development uses at least three independent runs per case, track, and declared
profile. Publication uses at least 30 fresh independent runs or cohort episodes
per required arm, randomized by a recorded seed after both adapters conform.
The hundreds of work items inside one outage or overload episode are correlated
observations, not independent trials.

The full population retains valid passes, valid failures, invalid runs, and
preregistered exclusions. Confidence intervals and recovery curves operate at
the independent run or episode level. The deterministic simulator establishes
mechanism causality; live coding-agent repetitions are reported separately for
external validity.

## Frozen publication preregistration

The active machine-readable
[`adl.publication.v2` preregistration](../../benchmarks/agent-durability/publication-preregistration-v2.json)
was frozen at `2026-08-09T02:07:45Z`, before the admitted pilot and publication
result. The original
[`adl.publication.v1` preregistration](../../benchmarks/agent-durability/publication-preregistration-v1.json)
remains readable and preserved. V2 superseded it prospectively after the pilot
showed that the calibration-only 50 ms healthy-task bound was not a portable
infrastructure gate. Its contract, adapter-baseline, population-policy, runner,
binary, and pilot hashes are checked by Go tests. The pilot and publication
seeds were sampled independently and frozen before their respective evidence.

The population has 18 strata: six cases by unfaulted, unsafe, and protected
probes. Each stratum requires 30 valid matched pairs, or 540 primary pairs and
1,080 system executions across the suite. Ten additional pair slots per stratum
are predetermined reserves. A reserve may replace only an invalid pair, never a
valid failure or an unfavorable result, and the stratum stops after 30 valid
pairs or 40 attempted pairs.

Each pair runs both systems sequentially under the same prewarmed host envelope.
The first system is balanced 15/15 within the 30 primary slots, then all blocks
are permuted by the frozen SplitMix64/Fisher-Yates algorithm. Adapter setup,
teardown, randomization, and readiness checks are outside measured intervals.
UTC timestamps establish portable event identity; elapsed durations come from
Go's monotonic clock. Named barrier arrivals alone select faults. Deadlines only
bound liveness.

Primary metric families are fixed before measurement:

| Case | Primary estimands |
| --- | --- |
| ABA reacquisition | stale-action acceptance, recovery latency, end-to-end latency |
| Retry amplification | physical requests, amplification, retry delay, recovery latency, cost units |
| Outage/backlog recovery | peak QPS, retry concurrency, recovery latency, backlog integral, drain p90 |
| Backpressure | queue latency, end-to-end latency, throughput, admission rejection fraction |
| Poison isolation | physical requests, healthy-task latency, recovery latency, cost units |
| Silent progress | detection latency, recovery latency, legitimate-wait revocation, stale-action acceptance |

Binary outcome rates receive Wilson 95% intervals. Efficiency uses paired
20,000-resample percentile-bootstrap intervals for median differences, with the
frozen analysis seed. Ratios are reported only when both paired values are
positive. These are descriptive metric families: there is no null-hypothesis
winner test, scalar score, or multiplicity-dependent declaration.

Unsafe controls are required to distinguish but are not efficiency-eligible.
An unfaulted or protected stratum receives an efficiency comparison only when
all 30 pairs are valid and both systems pass correctness, safety, liveness, and
diagnosability in every pair. Otherwise the efficiency question for that
stratum remains unresolved without filtering failed observations.

Pilot evidence is permanently labeled and excluded from publication. Pilots v1
through v3 remain preserved: v1 exposed the 50 ms calibration-bound error, v2
exposed a durable identity collision, and v3 exposed fail-open pair admission
plus a scheduler-sensitive outage control. The uniquely namespaced
[`pilot v4`](../../benchmarks/agent-durability/evidence/publication-v2-pilot-20260809-v4)
admitted all 54 pairs and 108 system runs. The
[`post-pilot freeze`](../../benchmarks/agent-durability/publication-harness-freeze-v2.json)
then pinned the exact runner source, binary, preregistration, and pilot
inventory before publication execution. No estimand, admission rule, schedule,
exclusion, or statistical method changed after publication results began.

## Durable execution order

The Beads graph is the authoritative tracker. `temporal_projects-zfz.1` freezes
the contract and schema. The ABA case (`zfz.2`) and common recovery harness
(`zfz.3`) depend on it. Temporal (`zfz.4`) and PostgreSQL (`zfz.5`) v2 adapters
depend on those common cases and their respective v1 adapters. The comparison
(`zfz.6`) depends on both required v2 adapters. Restate (`zfz.7`), DBOS
(`zfz.8`), and orchestration topology (`zfz.9`) are explicit follow-ups and do
not change this goal's required system scope.

Beads Dolt synchronization to the configured remote is authorized for this
goal. Git commits, Git pushes, and external publication remain outside scope
without fresh approval.

## Publication result

The frozen randomized population is
[`publication-v2-20260809-v1`](../../benchmarks/agent-durability/evidence/publication-v2-20260809-v1):
540 executed-valid pairs, 1,080 valid system executions, no invalid pair, and
180 reserve slots excluded by the preregistered stop rule. Every unfaulted and
protected system execution passed correctness, safety, liveness, and
diagnosability; every unsafe control distinguished. All 13,503 inventory entries
rehashed, all 540 Temporal histories replayed before admission, and all 540
PostgreSQL journals were retained.

The corrected
[`analysis v5`](../../benchmarks/agent-durability/evidence/publication-v2-20260809-v1-analysis-v5.json)
contains Wilson outcome intervals, paired distributions, and 20,000-resample
median-difference intervals. Earlier analysis outputs remain preserved: the
first omitted ABA's `accepted` completion label, and v2's supporting execution
span included retry waits. V3 corrected both metric reconstructions; v4 adds
strict rejection of uninventoried files and pins the analyzer binary. V5
confines all legacy stored paths to the supplied sealed root and exactly checks
the sealed schedule and population order against the preregistration. V5
reconstructs ABA completion from the accepted generation-9 request and
execution from cumulative authorized request intervals, as the frozen
definitions require. It changes no raw evidence,
primary estimand, admission, schedule, or statistical method.

Both systems compose with the portable fencing and recovery policies. The
pinned local Temporal arm shows higher queue/retry/recovery delay than the
PostgreSQL arm while attempts, admission decisions, cost units, and active
dependency time are usually equal. This is not a scalar winner or a universal
deployment claim. See
[finding 0013](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md).
