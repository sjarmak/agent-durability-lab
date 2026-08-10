# Cross-system agent durability benchmark

This benchmark asks which parts of reliable external-agent execution come from
a durability system and which still require application or destination
mechanisms. It is not a feature checklist or a throughput leaderboard.

The initial required comparison is Temporal and a deliberately small PostgreSQL
queue/lease/outbox implementation; Restate and DBOS Go remain follow-up
adapters. Temporal and PostgreSQL pass both the development-conformance gate and
the 30-pair-per-stratum v2 publication population. The report makes no scalar
winner claim. Durable Task and AWS Step Functions are deferred until this first
wave exposes where an additional architecture would change a decision.

The machine-checked case list and evidence requirements are in
[`contract-v1.json`](contract-v1.json).

Contract v1 remains frozen. The active, side-by-side
[`contract-v2.json`](contract-v2.json) and
[v2 research plan](../../docs/plans/agent-durability-benchmark-v2.md) add an ABA
authority case and recovery-dynamics profiles for retry amplification,
outage/backlog recovery, backpressure, poison work, and silent progress. They do
not reinterpret v1 evidence or expand the v1 publication population.

The separate, frozen
[`topology-contract-v1.json`](topology-contract-v1.json) and
[`topology-preregistration-v1.json`](topology-preregistration-v1.json) define a
within-Temporal comparison: one parent Workflow scheduling the common
work Activity directly versus one parent scheduling a Child Workflow per item,
where every child schedules that identical Activity. The fixed 8/32/128 fan-out
ladder covers joins, partial reduction, queued/executing supersession,
destructive transitions, and the full crash, retry, outage, backpressure,
poison, and silent-progress recovery suite. The [shared topology
foundation](topology/README.md) now implements deterministic paired scheduling,
stable identities, append-only evidence, exact-barrier process integration, and
fail-closed admission. The first four semantics cases now also have a preserved
44-run canonical development suite with native history replay and distinguishing
controls; see [finding 0015](../../docs/findings/0015-topology-semantics-controls-distinguish-with-replay.md).
The six recovery-dynamics cases have a separate 52-run canonical mechanism
suite covering all five crash windows, bounded retry, outage/backlog catch-up,
backpressure, poison isolation, and silent progress; see
[finding 0016](../../docs/findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md).
The complete 88-stratum schedule and apparatus are now admitted through an
integrated fixture matrix, four invalid controls, and 23 predetermined live
Temporal sentinel pairs; see
[finding 0018](../../docs/findings/0018-topology-measurement-admission-is-independent-before-pilot.md).
The final review reconstructs every registered metric from sealed raw evidence,
rejects fixture history in live runs, and globally balances pilot arm order.
Those fixtures and sentinels are publication-excluded. The independent pilot,
scale population, performance evidence, and any supported topology comparison
remain pending.

The v2 apparatus is executable. Generate all six cases, three probes, and three
development trials without overwriting prior evidence:

```bash
go run ./benchmarks/agent-durability/v2/cmd/calibrate \
  --evidence-root benchmarks/agent-durability/evidence/<new-v2-suite-id> \
  --trials 3
```

The current preserved apparatus suite is
[`calibration-v2-20260808-v1`](evidence/calibration-v2-20260808-v1): 36 expected
passes, 18 distinguishing valid failures, and zero invalid runs. The preserved
real-process ABA suite is
[`live-aba-v2-20260808-v1`](evidence/live-aba-v2-20260808-v1): three label-only
failures and three generation/capability-fenced passes. These establish case and
oracle behavior only; neither directory is evidence about a durability system.

The required-system development populations are preserved at
[`temporal-v1-20260808-v1`](evidence/temporal-v1-20260808-v1),
[`postgresql-v1-20260808-v1`](evidence/postgresql-v1-20260808-v1),
[`temporal-v2-20260808-v3`](evidence/temporal-v2-20260808-v3), and
[`postgresql-v2-20260808-v2`](evidence/postgresql-v2-20260808-v2). Temporal v2
contains replayed Event Histories; PostgreSQL v2 contains transactional queue,
lease-expiry, reacquisition, and acknowledgement journals. See
[finding 0012](../../docs/findings/0012-temporal-and-postgresql-pass-development-conformance-not-performance.md)
for the earlier development boundary. The fresh system-timed population is
[`publication-v2-20260809-v1`](evidence/publication-v2-20260809-v1), with the
corrected preregistered uncertainty report in
[`analysis v5`](evidence/publication-v2-20260809-v1-analysis-v5.json). [Finding
0013](../../docs/findings/0013-application-policy-equalizes-safety-not-recovery-cost.md)
states the supported result and its single-host limits.

## Credential-safe publication invocation

Publication and live adapter targets accept a libpq service name, not a raw
connection string. Put connection parameters in `pg_service.conf` and keep the
password in `.pgpass` or a mode-0600 file named by `PGPASSFILE`. This keeps
credentials out of the runner and `psql` process arguments. For a new
append-only pilot or publication root, run:

```bash
make publication-v2 \
  PHASE=pilot \
  EVIDENCE_ROOT=benchmarks/agent-durability/evidence/<new-population-id> \
  TEMPORAL_WORK_ROOT=benchmarks/agent-durability/evidence/<new-population-id>-system-work \
  TEMPORAL_CLI_PATH="$(command -v temporal)" \
  POSTGRES_SERVICE=agent_durability_v2
```

The post-pilot runner is cryptographically frozen, so its lower-level
`--postgres-dsn` flag remains for source replay. Pass only a non-secret
`service=<name>` reference to that flag; never place a password in it.

## Comparison unit

The unit is a complete run of the same external application under one exact
fault schedule—not a framework function or marketing guarantee. Every adapter
must use the same:

- deterministic agent binary and session protocol;
- application authority-store protocol;
- external effect destination and physical-attempt journal;
- named barrier service and process controller;
- case input, seed, fault boundary, and timeout ceiling;
- evidence envelope and independent oracle; and
- host resource envelope.

Adapters may use their system idiomatically for durable procedure. Temporal may
choose Workflow plus Activities or a declared Standalone Activity variant;
Restate may use a service, Workflow, durable `Run`, state, and promises; DBOS may
use workflows, steps, queues, and declared datasource transactions; PostgreSQL
may use transactions, `FOR UPDATE SKIP LOCKED`, leases, and an outbox. An adapter
may not replace the common agent, destination, failure controller, or oracle.

## Three tracks prevent category errors

Each system starts with two required tracks and may add a third:

1. `native-minimum` uses only the durability primitive and the minimum glue
   needed to call the common workload. It is expected to expose boundaries such
   as effect-success/completion-loss. This is a control, not a recommended
   production design.
2. `portable-safety` adds the same stable identity, application fence,
   idempotency/reconciliation, and cancellation protocol for every system. This
   asks whether the common application mechanisms compose with recovery.
3. `native-optimized` is optional and separately labeled. It may use a unique
   co-transactional primitive—for example a DBOS datasource transaction or a
   PostgreSQL transaction that atomically changes application and outbox state.

Results never compare a native optimized arm against another system's native
minimum arm as if the durability products alone differed. Every guarantee is
tagged `system`, `application`, `destination`, `operating-system`, or a named
combination.

## Cases and exact oracles

The first four cases come directly from failures already reproduced in this lab:

| Case | Injected boundary | Primary oracle |
| --- | --- | --- |
| `surviving-executor` | Kill the system executor after the external agent registers and blocks before effect | Competitor count, stable session identity, accepted outcome count |
| `ambiguous-effect` | Destination confirms an effect; kill before durable step completion | Destination physical attempts versus logical effects and durable completions |
| `stale-generation` | Replace generation 1, then deliver its delayed effect, completion, and stop | Every obsolete authoritative action rejected; generation 2 remains alive |
| `cancellation-unreachable` | Freeze the exact process tree, cancel, then resume it | Revocation precedes acknowledgement; no post-revocation mutation or replacement |

The oracle runs outside the adapter and reads the common authority store,
destination journal, process observations, fault record, and native history
export. Adapter logs can explain a verdict but cannot determine it. A run is
invalid, not failed, if the barrier was missed, the wrong PID/start identity was
targeted, required evidence is absent, or the fault did not occur between the
two declared events.

## Case and oracle admission

The benchmark tests its apparatus before it compares systems. Each case must
pass four admission probes:

1. An unfaulted calibration run reaches the expected output under every adapter.
2. A deliberately unsafe common control, separate from system scoring, violates
   the named invariant at the dangerous boundary. A false pass may mean the
   fault missed, the oracle leaked recovery information, or the case cannot
   distinguish the mechanism. A system's `native-minimum` arm is still allowed
   to satisfy the invariant through a genuine native guarantee.
3. The common `portable-safety` reference satisfies the invariant without using
   adapter logs as its oracle.
4. Missed-boundary, wrong-identity, incomplete, and deliberately altered
   evidence fixtures are rejected as invalid rather than scored as passes or
   failures.

The case manifest, common protocol, oracle, fixtures, and expected unsafe
failure are versioned together. A maintained case may change after a defect is
found, but published results continue naming the frozen contract that produced
their run population.

Every adapter exports an effective-input inventory before launch. It includes
system and SDK versions, binary and container hashes, retry and timeout policy,
credentials and permissions, host limits, fault-controller version, destination
identity, agent binary, and oracle visibility. Controls that the adapter cannot
enforce cause preflight refusal. Similar configuration names are not accepted as
evidence of parity.

## Trial and statistical policy

Development uses three trials per arm to catch deterministic contract errors.
A publishable cross-system result requires at least 30 fresh trials per arm,
randomized system/arm execution order with a recorded seed, identical host
limits, and all raw trials retained. Report counts and empirical distributions;
do not hide invalid or failed trials and do not report only successful reruns.

Every trial receives one of three primary verdict classes: `valid-pass`,
`valid-fail`, or `invalid`, plus a mechanical reason code. Infrastructure and
apparatus failures do not silently become zero-quality results. They remain in
the structural run counts and make the run set non-quotable until handled by the
rerun or exclusion policy declared before results were inspected.

Correctness is evaluated per run before aggregation. Recovery latency, durable
bytes, external calls, or operational footprint are compared only among arms
that reach output parity. This avoids declaring a system cheaper or faster
because it silently lost work. Confidence intervals are required for failure
rates and latency quantiles once the sample size supports them; three-trial
development results are not percentages.

## Measurements tied to decisions

- Invariant pass/fail, physical effect count, stale accepts, terminal outcomes,
  and cancellation stages decide whether an architecture is safe enough to
  consider.
- Fault-to-revocation and fault-to-outcome latency decide whether recovery meets
  an operator's service objective after correctness parity.
- Durable records/bytes and external calls identify history or journal pressure
  and destination amplification.
- Operator interventions and unrecoverable/wedged runs decide whether automated
  recovery is operationally sufficient.
- Adapter code/configuration surface, required services, schema migrations, and
  upgrade constraints expose complexity. They remain separate observations,
  not a subjective composite score.

CPU microbenchmarks and synthetic no-op throughput are excluded from v1 because
they do not answer the reliability decision and would be dominated by the
single-host harness.

## Current product hypotheses, not results

Primary documentation defines what each first adapter should attempt; it is not
benchmark evidence:

- Temporal documents durable Workflow procedure, Activity retry, and
  heartbeat-mediated cancellation. The adapter must still reproduce the
  external effect and process boundaries rather than infer them from Event
  History. See [Temporal Activity failure detection](https://docs.temporal.io/develop/go/failure-detection)
  and the pinned [Go SDK Activity options](https://pkg.go.dev/go.temporal.io/sdk/workflow#ActivityOptions).
- Restate records context actions and durable `Run` results in an execution
  journal. The benchmark hypothesis is that a crash after an external effect but
  before the corresponding journal entry can still expose ambiguity. See
  [Restate durable steps](https://docs.restate.dev/develop/go/durable-steps) and
  [architecture](https://docs.restate.dev/references/architecture).
- DBOS Go documents at-least-once steps and atomic application/durability commits
  for supported datasource transactions. Those belong in separate minimum and
  native-optimized arms. See [DBOS steps](https://docs.dbos.dev/golang/tutorials/step-tutorial),
  [transactions](https://docs.dbos.dev/golang/tutorials/transaction-tutorial),
  and [workflow recovery](https://docs.dbos.dev/production/workflow-recovery).
- PostgreSQL provides transactional rows and `SKIP LOCKED`, explicitly noting
  that the latter presents an inconsistent view suitable for queue-like access.
  Every lease, recovery, fence, timer, and outbox guarantee is therefore our
  implementation responsibility. See [PostgreSQL `SELECT` locking](https://www.postgresql.org/docs/current/sql-select.html).

No cross-system winner or unimplemented system guarantee is claimed here.

## Adapter conformance gate

Before measurements count, an adapter must:

1. pass protocol validation against the common simulator and destination;
2. execute all four faults at the named boundaries without sleeps choosing the
   outcome;
3. export the native durable record plus every common evidence file;
4. pass the independent oracle in an unfaulted calibration run;
5. preserve failed and invalid evidence;
6. pin system, SDK, database, adapter commit, OS, and binary hashes; and
7. document every non-default retry, timeout, retention, recovery, and
   deployment setting.

Only adapters that pass this gate enter a result table. The completed v2
required-system table covers Temporal and PostgreSQL; later Restate or DBOS
results must pass independently and may not reinterpret this frozen population.

## Common harness status

The adapter-neutral v1 harness is implemented in five deliberately separated
packages:

- `protocol` owns the fixed identities and strict evidence schemas;
- `evidence` publishes typed, append-only raw bundles and has no verdict API;
- `calibration` produces deterministic apparatus fixtures through that writer;
- `livecommon` drives the real simulator, Bolt store, named barriers, and exact
  Linux process control while retaining adapter-owned orchestration; and
- `oracle` verifies file hashes, fault bracketing, stable process identity, and
  agreement between events, authority state, and the destination journal before
  evaluating a case invariant.

The deterministic calibration adapter is apparatus testing, not evidence about
Temporal or another durability system. Its unsafe probe must fail, while its
unfaulted and portable-safety probes must pass. Generate a fresh append-only
suite with:

```bash
go run ./benchmarks/agent-durability/cmd/calibrate \
  -evidence-dir benchmarks/agent-durability/evidence/<new-suite-id>
```

The current preserved suite is
[`calibration-20260807-v4`](evidence/calibration-20260807-v4). Across three
trials for every case and probe, the independent oracle recorded 24
`valid-pass`, 12 expected `valid-fail`, and zero invalid runs. Adversarial tests
also prove that changed hashes, wrong named barriers, malformed boundary times,
incorrect or empty process identity, changed protocol IDs, omitted accepted
actions, absent cancellation, unreported competitors, contradictory active
generations, unknown accepted events, post-cancellation outcomes, unfrozen
cancellation targets, and
self-reported authority inconsistent with the event stream are rejected or
scored as failures as the contract requires.

Suites [`v1`](evidence/calibration-20260807-v1),
[`v2`](evidence/calibration-20260807-v2), and
[`v3`](evidence/calibration-20260807-v3) are retained but superseded. Review of
v1 found fail-open schema and boundary validation. Later adversarial passes
found that active-generation, competitor, fault-target, unknown-event,
post-cancellation-outcome, and frozen-process contradictions were not all
derived independently. No suite was rewritten; v4 was generated after every
review finding was encoded as a regression test.

The common live conformance fixture is also complete. Build the simulator and
run a source-pinned suite with:

```bash
go build -o /tmp/agent-simulator ./cmd/agent-simulator
go run ./benchmarks/agent-durability/cmd/live-common \
  -agent-binary /tmp/agent-simulator \
  -adapter-version source-sha256:<immutable-source-hash> \
  -evidence-dir benchmarks/agent-durability/evidence/<new-live-suite-id>
```

The corrected preserved suite is
[`live-common-20260807-v2`](evidence/live-common-20260807-v2): 24
`valid-pass`, 12 intentional `valid-fail`, and zero invalid runs across three
live trials for every case and probe. The first live suite remains preserved but
is superseded because it recorded `v1` rather than an immutable adapter source
identity. [Finding 0007](../../docs/findings/0007-live-common-harness-calibrates-the-oracle.md)
states the supported apparatus claim and its limits.

System and vendor coding-agent adapters now reuse this boundary, adding native
durability records plus transcript, vendor-session, sandbox, worktree, and
tool-call identities rather than creating another benchmark contract.
