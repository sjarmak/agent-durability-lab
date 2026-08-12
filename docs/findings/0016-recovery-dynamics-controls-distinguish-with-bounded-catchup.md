# Finding 0016: Recovery dynamics controls distinguish with bounded catch-up

## Observation

The six frozen Temporal topology recovery cases now execute in both scheduling
arms through a real Temporal service, loopback dependency service, restartable
SDK Workers, and detached hermetic agent processes. The Direct-Activity arm
runs the common per-item procedure in the parent Workflow. The Child-Workflow
arm adds one item Child Workflow which runs that same procedure. Both use the
same Work Activity, inputs, options, queues, retry owner and budgets, authority
and destination protocol, and Worker Activity concurrency of eight.

The accepted append-only development root
[`recovery-20260811-v7`](../../benchmarks/agent-durability/topology/evidence/recovery-20260811-v7)
contains one canonical fan-out-32 pair for all 26 recovery
case/boundary/probe combinations: 52 runs and 780 inventory-sealed artifacts.
All 52 runs were valid, every admitted item reached an explicit succeeded or
quarantined terminal disposition, and every captured parent plus actual Child
Workflow history replayed. The 32 unfaulted/protected runs passed correctness,
safety, liveness, and diagnosability. The 20 unsafe runs remained admissible
logical results and failed safety.

The protected runs stayed within the preregistered bounds: at most four
physical requests per item for layered retry, at most two simultaneous catch-up
requests, at most eight admitted outstanding items for overload, three poison
attempts followed by quarantine, and wedge detection before the 5,000 ms
progress deadline with no declared-wait revocation. The corresponding unsafe
controls exposed their prohibited behavior in both topology arms: request
amplification reached 256 total requests rather than 128, catch-up
concurrency reached eight, overload admitted 32 items, poison work consumed five
attempts, and wedge detection exceeded its deadline. Each of the five unsafe
crash-window pairs also recorded one duplicate effect and result for the
designated item; the protected pairs recorded none.

Two earlier partial roots remain untouched and are rejected by sibling failure
records. [`recovery-20260809-v1`](../../benchmarks/agent-durability/topology/evidence/recovery-20260809-v1)
held an application admission token while outage Activities waited for all 32
items, preventing the remaining backlog from registering.
[`recovery-20260809-v2`](../../benchmarks/agent-durability/topology/evidence/recovery-20260809-v2)
changed recovery Worker concurrency to 16 instead of the frozen eight and still
retained scarce Activity slots during global coordination. The accepted design
makes admission a durable Activity, returns an intermediate retry disposition
instead of waiting inside outage Activities, and lets the common Workflow
procedure schedule later Activity rounds. A subsequent fan-out-8 test exposed
and corrected a separate pre-registration attach error: blocked-registration
state now belongs to the concrete controlled process rather than the scenario
label.

The complete
[`recovery-20260809-v3`](../../benchmarks/agent-durability/topology/evidence/recovery-20260809-v3)
root also remains unchanged and passed its own inventory and replay audit, but
is superseded for the current claim. The subsequent repository race gate found
that unrelated items contended on one bbolt file, retry permits remained held
after dependency requests, physical-request budgets were attempt-local, and
detached children were released rather than reaped. The corrected apparatus
uses one durable store per stable item, releases retry permits after the
physical request, counts requests across Activity attempts, and asynchronously
reaps detached children. Exact boundary repetitions and the full recovery
matrix passed after those fixes.

The complete
[`recovery-20260809-v4`](../../benchmarks/agent-durability/topology/evidence/recovery-20260809-v4)
root remains unchanged as well. A subsequent full race-and-coverage gate found
one protected Child-Workflow silent-progress run whose revocation was recorded
after 5,000 ms: the four-second Workflow timer left only one second for Activity
dispatch under instrumentation. The failure is preserved in the bead. The
corrected policy reserves two seconds for dispatch without changing the
registered five-second deadline. The exact protected boundary passed five
race-enabled repetitions in both arms, the full topology race gate passed at
84.3% combined statement coverage, and v5 captured the corrected behavior.

The complete
[`recovery-20260809-v5`](../../benchmarks/agent-durability/topology/evidence/recovery-20260809-v5)
root remains unchanged. Final static review after that run removed two unused
recovery-state fields and made ignored HTTP response-body close errors explicit.
Although behavior-neutral, those cleanups changed the executable digest and
required v6.

During coding-agent cookbook integration, the final independent oracle rejected
one v6 unsafe outage run with `recovery_metric_mismatch`: v6 preceded the
current raw-metric reconstruction even though it had passed its contemporary
audit. The v6 root remains unchanged as superseded evidence. V7 is the
source-pinned population from the then-final harness and oracle bytes. Its
runner reported 26 pairs, 52 preserved runs, 32 valid passes, 20 valid
failures, and zero invalid/errors; a separate disk-only gate recomputed every
inventory and verdict and replayed every captured history. Later
supersession-only cancellation/replay hardening changed the shared executable
digest without changing the recovery mechanism or the preserved root. V7
therefore remains admitted historical mechanism evidence, but it is not
current-source evidence for the present worktree; renewing that claim requires
a fresh append-only population.

## Inference

All six recovery mechanisms are executable and distinguishing in both topology
shapes at the canonical development scale. Under the tested protected protocol,
stable logical identity, start-or-attach, bounded retry ownership, durable
admission, deterministic jitter, poison quarantine, explicit progress
deadlines, generation/capability replacement, and destination fencing compose
with Temporal recovery without violating their registered invariants. Outage
recovery can accumulate the full backlog, restore the service, lose a Worker on
the first accepted catch-up request, and drain without a second retry storm.

This is not evidence that either topology is faster or cheaper. There is one
development pair per mechanism combination, not a repeated 8/32/128 population,
and the observed latency/history differences have no inferential analysis. It
is also not an exactly-once result. Temporal records durable procedure and
redelivers incomplete Activities; the application work store and destination
generation/capability and receipt protocols reject or reconcile repeated
external actions.

## Responsibility boundary

Temporal supplies durable Workflow, Child Workflow, Activity, timer,
cancellation, and replayable Event History behavior. Application code supplies
immutable membership, stable operation/session identity, one retry owner and
budget, exact admission accounting, deterministic jitter, process lifecycle,
per-item durable store layout, poison disposition, progress meaning, and fenced replacement. The dependency
service supplies observable physical request/outage behavior. The work store
and destination supply atomic start-or-attach, authority comparison, effect
acceptance, and receipt reconciliation. The harness supplies named barriers,
Worker restart, real process/service coverage, append-only evidence, and the
independent causal oracle.

The observed pass is therefore a composition result. It does not promote
application admission, retry, fencing, or process-discovery policy into a
native Temporal guarantee.

## Falsifier

This finding is false if any stored v7 inventory fails verification or replay;
if either topology receives different Work input/options, retry policy, Worker
concurrency, authority, or destination semantics; if a protected run loses or
double-accounts an item, exceeds a registered request/concurrency/admission/
poison/progress bound, accepts a stale or duplicate effect/result, falsely
revokes a declared wait, fails to quarantine poison, or fails to recover after
outage plus Worker loss; or if an unsafe control ceases to expose its prohibited
outcome. A scale-population, relative-cost, or exactly-once claim based on this
single development root would also exceed the evidence.
