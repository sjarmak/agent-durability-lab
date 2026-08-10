# Temporal topology benchmark foundation, semantics, and recovery suites

This package implements the shared pre-pilot boundary for the frozen
Direct-Activity versus Child-Workflow comparison. It proves that both arms can
be scheduled, identified, recorded, replayed, and admitted through one common
protocol. The first four orchestration-semantics cases and all six
recovery-dynamics cases are executable. The complete frozen 88-stratum
apparatus matrix and a predetermined live sentinel set now pass integrated
conformance. The independent pilot, publication population, and topology
comparison remain pending.

## Question and invariant

The foundation asks whether the harness can keep the two arms identical except
for the parent-to-work scheduling edge and fail closed when that comparison is
not trustworthy.

The invariant is that one frozen pair names the same case, boundary, probe,
fan-out, workload, Activity options, host envelope, agent binary, destination
protocol, barrier controller, and source identity. The Direct-Activity arm has
no Child Workflow identity. The Child-Workflow arm has one, but it receives no
additional retry policy or business behavior. Activity attempt is delivery
identity; the logical operation and item remain stable across attempts, while
generation and capability identify the authority actually used by each event
and may advance after fencing or reacquisition.

## Failure boundary and oracle

The tested boundary starts at frozen schedule admission and crosses readiness,
the hermetic agent-process launch, exact barrier arrival, evidence publication,
history replay status, independent reconstruction, and pair sealing. Faults are
selected by named barrier arrival from the exact process PID/start identity,
never by elapsed-time guesses.

The independent oracle reads the sealed raw files and derives admission plus
correctness, safety, liveness, and diagnosability. A logical failure remains an
admitted observation. Missing lineage, a missed or wrong barrier, incompatible
history replay, mismatched arm inputs, evidence outside the caller root,
schedule drift, and outcome-derived exclusions make the run or pair invalid.
The paired runner still executes the second arm after a first-arm logical
failure.

## Run commands and evidence location

Run the complete source-level acceptance suite with:

```bash
go test -race ./benchmarks/agent-durability/topology/...
go test -race \
  -coverpkg=./benchmarks/agent-durability/topology/agent,./benchmarks/agent-durability/topology/evidence,./benchmarks/agent-durability/topology/internal/sealedfs,./benchmarks/agent-durability/topology/matrix,./benchmarks/agent-durability/topology/oracle,./benchmarks/agent-durability/topology/protocol,./benchmarks/agent-durability/topology/runner,./benchmarks/agent-durability/topology/semantics \
  -coverprofile=coverage.topology.out \
  ./benchmarks/agent-durability/topology/...
```

Run a new append-only canonical mechanism suite with separate evidence and work
roots:

```bash
make topology-semantics-conformance \
  EVIDENCE_ROOT=benchmarks/agent-durability/topology/evidence/<new-suite-id> \
  TEMPORAL_WORK_ROOT=/tmp/<new-topology-work-id> \
  TEMPORAL_CLI_PATH="$(command -v temporal)"
```

Run the canonical recovery mechanism suite at the frozen fan-out of 32 with:

```bash
make topology-recovery-conformance \
  EVIDENCE_ROOT=benchmarks/agent-durability/topology/evidence/<new-recovery-suite-id> \
  TEMPORAL_WORK_ROOT=/tmp/<new-topology-recovery-work-id> \
  TEMPORAL_CLI_PATH="$(command -v temporal)"
```

Run a fresh integrated matrix conformance root with the exact frozen schedule,
all 88 deterministic stratum fixtures, four invalid controls, and the
predetermined real Temporal sentinel set with:

```bash
make topology-matrix-conformance \
  EVIDENCE_ROOT=benchmarks/agent-durability/topology/evidence/<new-matrix-suite-id> \
  TEMPORAL_WORK_ROOT=/tmp/<new-topology-matrix-work-id> \
  TEMPORAL_CLI_PATH="$(command -v temporal)"
```

All three evidence targets build the bound harness and agent executables with
`go build -trimpath`. This is part of the provenance contract: rebuilding the
accepted matrix from its current source and module state reproduces the two
recorded executable digests byte for byte.

The accepted 2026-08-09 development root is
[`semantics-20260809-v2`](evidence/semantics-20260809-v2). It contains 44
append-only run directories: every one of 22 case/boundary/probe combinations
in both topology arms at fan-out 32. Each run has 15 inventory-sealed files,
including causal events, exact fault data, process identities, destination and
dependency observations, five case metrics, and native history. This is
mechanism-conformance evidence, not pilot, publication, or performance evidence.
Its effective-input and replay records bind the SHA-256 digest of the actual
conformance executable; the agent field independently binds the simulator.

The accepted recovery mechanism root is
[`recovery-20260809-v6`](evidence/recovery-20260809-v6). It contains 52
append-only run directories: all 26 frozen recovery case/boundary/probe
combinations in both topology arms at fan-out 32. Each run carries the same 15
inventory-sealed files, plus the case-specific recovery item ledger, retry and
dependency records, decomposed latency/load/history metrics, exact fault data,
real process identities, and captured native history. This is recovery
mechanism-conformance evidence, not a repeated scale population or relative
cost result.

The accepted integrated apparatus root is
[`matrix-20260809-v7`](evidence/matrix-20260809-v7). It seals the complete
3,520-block schedule for 88 strata, including exactly 2,640 primary and 880
reserve pairs, 5,280 primary arm executions, and balanced first-arm order.
The executed apparatus subset contains 88 deterministic fixture pairs, four
invalid controls, and 23 predetermined live Temporal sentinel pairs. All 38
unsafe fixture arms and all 38 unsafe live arms distinguished; all 138
protected/unfaulted fixture arms and all eight protected/unfaulted live arms
passed correctness, safety, liveness, and diagnosability; all four invalid
controls were rejected; and all 46 live histories replayed. The root contains
3,799 files, with the final inventory sealing the other 3,798 artifacts and
123,887,544 bytes. The report labels the entire root `publication_excluded`:
these are apparatus fixtures and sentinels, not independent paired episodes.

[`matrix-20260809-v1`](evidence/matrix-20260809-v1) remains intact but is
superseded because final review subsequently made recovery/semantics metric
admission independent of self-reporting, hardened root and pair audit, added a
real Temporal readiness RPC, and bound all executable digests. The partial
[`matrix-20260809-v2`](evidence/matrix-20260809-v2) and
[`matrix-20260809-v3`](evidence/matrix-20260809-v3) roots are rejected and
preserved with `failure.json`, the invalid pair, parent and all 32 Child
Workflow histories, and SHA-256 manifests. They exposed retry-loop starvation
of the exact outage-backlog barrier under accumulated history. The corrected
common procedure uses a versioned, effect-queue cohort gate so waiting items do
not occupy the frozen eight Work Activity slots. Complete
[`matrix-20260809-v4`](evidence/matrix-20260809-v4) passed the full sequence but
is superseded because the subsequent repository race gate exposed that procfs
can report a normally exited process with `ESRCH`, which the shared process
probe did not yet classify as gone. [`matrix-20260809-v5`](evidence/matrix-20260809-v5)
contains the same matrix after that race-safe lifecycle correction, but is now
superseded for current-source claims by the final measurement-admission review.
That review globally balanced the three-pair pilot order, pinned the exact
preregistration bytes, reconstructed every registered metric and history count
from raw causal/request/history evidence, fixed peak QPS to the registered
10-millisecond window, rejected synthetic fixture histories from live runs, and
distinguished silent-progress deadline detection from generic recovery
telemetry. [`matrix-20260809-v6`](evidence/matrix-20260809-v6) is a preserved
rejected preflight root: a wrong preregistration path created the directory but
executed no episode. V7 is the corrected append-only rerun.

The earlier
[`semantics-20260809-v1`](evidence/semantics-20260809-v1) root remains intact,
but is superseded for claims because its `source_sha256` was derived from a
version label and its replay-worker digest named the simulator rather than the
replay executable. No raw artifact was deleted or rewritten.

The partial [`recovery-20260809-v1`](evidence/recovery-20260809-v1) and
[`recovery-20260809-v2`](evidence/recovery-20260809-v2) roots and complete
[`recovery-20260809-v3`](evidence/recovery-20260809-v3) and
[`recovery-20260809-v4`](evidence/recovery-20260809-v4) and
[`recovery-20260809-v5`](evidence/recovery-20260809-v5) roots also remain
unchanged. Sibling `failure.json` records reject v1 and v2: v1 deadlocked outage
registration behind an application semaphore, while v2 changed Worker
concurrency from the frozen eight to 16 and still retained scarce Activity
slots during global backlog coordination. V3 passed its own audit but is
superseded because the subsequent repository race gate exposed shared bbolt
store contention, held retry slots, attempt-local request accounting, and
unreaped detached children. V4 corrected those mechanisms, then the full race
gate exposed that its protected silent-progress timer reserved only one second
of the five-second bound for Activity dispatch. V5 was generated after a tested
two-second dispatch margin passed five exact-boundary race repetitions and the
full race gate. Final static review after v5 removed two unused fields and made
HTTP response-body close handling explicit. Those behavior-neutral changes
altered the bound executable, so v6 is the fresh root. None of v1 through v5
supports the current topology claim.

## Observed results

On 2026-08-10, the final race-enabled suite passed with 82.8% combined
statement coverage across the topology production packages, including the
integrated matrix package. The real-process test builds
the existing hermetic `agent-simulator`, launches it as
a detached Linux process for both topology identities, observes its exact
PID/start identity at the HTTP barrier service, releases the named barriers,
and reconciles one durable effect and outcome. Adversarial tests independently
make every locked admission defect invalid. See
[finding 0014](../../../docs/findings/0014-topology-foundation-fails-closed-before-pilot.md).

The semantics extension executes join/barrier, incremental reduction,
queued/executing supersession, and destructive transition under unfaulted,
unsafe, and protected paths at every frozen primary and secondary boundary.
The preserved canonical run admitted all 44 histories and replayed every parent
and actual Child Workflow history. All 26 unfaulted/protected arms passed the
four outcome dimensions; all 18 unsafe arms remained valid evidence and failed
safety. Thirty-six faulted arms contain an exact barrier/fault/recovery bracket.
See [finding 0015](../../../docs/findings/0015-topology-semantics-controls-distinguish-with-replay.md).

The recovery extension executes all five crash windows, bounded layered retry,
outage accumulation/restoration with a catch-up Worker crash, overload
admission, poison isolation, and silent-progress replacement. The accepted
canonical run preserved and independently re-audited all 52 histories. All 32
unfaulted/protected arms passed all four outcome dimensions; all 20 unsafe arms
remained valid and failed safety. Every admitted item reached one explicit
terminal disposition, and every parent plus actual Child Workflow history
replayed. See [finding 0016](../../../docs/findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md).

The integrated conformance command reconstructs the complete frozen schedule,
runs one deterministic pair in every stratum, rejects four independently
corrupted controls, and then runs 23 predetermined real Temporal sentinel
pairs. The accepted root passed a second disk-only audit that reconstructed
schedule arithmetic, pair membership and matched inputs, verdicts, replay
records, inventories, and executable provenance without trusting the aggregate
report. See [finding 0017](../../../docs/findings/0017-topology-matrix-is-ready-for-pilot-not-publication.md).
The subsequent full measurement-admission review and accepted v7 evidence are
recorded in [finding 0018](../../../docs/findings/0018-topology-measurement-admission-is-independent-before-pilot.md).

## Responsibility split

- Temporal supplies durable Workflow, Child Workflow, Activity, timer,
  cancellation, and Event History behavior in the case implementations.
- Application code supplies stable identity, fixed membership, authority,
  retry ownership, deterministic scheduling inputs, and replay-compatible
  Workflow definitions.
- The destination supplies atomic fencing, idempotency/version checks, and
  durable receipts for authoritative effects.
- The shared harness supplies exact barriers, the hermetic process, append-only
  evidence, root confinement, randomized paired scheduling, and the independent
  oracle.

## Falsifier and remaining boundary

The foundation is falsified if either topology receives different work inputs
or policy, a corrupt run is admitted, a logical failure is filtered as
infrastructure, a schedule can drift from the frozen seed, or a returned
artifact can escape the sealed root. The semantics result is falsified if a
protected arm continues early, double-counts a contribution, accepts obsolete
authority, repeats a destructive apply, loses liveness, or cannot replay; it is
also falsified if an unsafe control fails to expose its prohibited outcome.

The mechanism and integrated apparatus suites establish no comparative
topology-cost claim and no population estimate across the frozen 8/32/128 scale
ladder. Those remain pilot, freeze, and publication work. No
exactly-once claim is made: protected effects depend on the application work
store and destination fence/receipt protocol, not Activity retry alone.

## Deferred population run

The timing-sensitive pilot, freeze verification, and 8/32/128 population run
are intentionally deferred. This workstation is shared with Gas City and other
coding-agent sessions, so host contention can change queue, retry, recovery,
and control-lane latency enough to invalidate a topology comparison. No new
evidence root or efficiency claim should be created from that environment.

Resume this work only on a controlled Linux amd64 host with 16 dedicated,
non-burstable vCPUs, 32 GiB of memory, at least 20 GiB of free local storage,
and a four-hour uninterrupted window. Before the run, require three samples 15
seconds apart with load average below 8 and CPU pressure-stall `avg10` below
5%. Keep hostname and visible CPU topology fixed across pilot and freeze
verification. Docker is acceptable on that controlled host when CPU and memory
limits are explicit; a container on this same contended workstation is not an
isolation boundary for the benchmark.

The future sequence is strict canary, replay/race/coverage/static gates, a fresh
append-only pilot, disk-only audit, then freeze verification. Abort rather than
admit the run if the host envelope drifts, the runner or Temporal services are
co-scheduled with unrelated work, an evidence episode is incomplete, replay or
audit fails, or a registered latency bound is exceeded. Provisioning a paid AWS,
Google Cloud, or other external host requires fresh explicit approval; until
approved or suitable already-owned compute is available, this work remains
TBD without blocking repository-local stabilization work.
