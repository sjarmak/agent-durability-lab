# Finding 0015: Topology semantics controls distinguish with replay

Twenty-two topology semantics cases ran through a real Temporal service in both
scheduling arms. Twenty-six unfaulted and protected runs passed. Eighteen
unsafe runs distinguished. All 44 histories replayed. This is mechanism
conformance.

## Observation

The first four frozen Temporal topology cases now run through a real Temporal
service and hermetic agent subprocess in both arms. The Direct-Activity parent
schedules the common Work Activity itself; the Child-Workflow parent schedules
one item child which schedules that same Activity with the same input, options,
authority, destination, and retry budget.

The accepted append-only development root
[`semantics-20260809-v2`](../../benchmarks/agent-durability/topology/evidence/semantics-20260809-v2)
contains one canonical fan-out-32 pair for all 22 orchestration-semantics
case/boundary/probe combinations: 44 runs and 660 sealed artifacts. All 44 runs
were valid, supplied five preregistered case metrics, and replayed their captured
parent plus actual Child Workflow histories. The 26 unfaulted/protected runs
passed correctness, safety, liveness, and diagnosability. The 18 unsafe runs
were admitted logical failures and failed safety. Thirty-six faulted runs record
the exact barrier, committed fault, recovery event, process identity, and causal
lineage.

Every v2 run binds both `source_sha256` and `replay_worker_sha256` to the
SHA-256 digest of the actual conformance executable and separately binds the
agent simulator. The preserved
[`semantics-20260809-v1`](../../benchmarks/agent-durability/topology/evidence/semantics-20260809-v1)
population has the same logical verdict counts but is superseded for claims:
its source field hashed a version label and its replay-worker field named the
simulator. It remains untouched as the original raw result.

Final product-integration verification later captured an ordering-sensitive
replay failure in an unfaulted Child-Workflow supersession run: durable fencing
could release obsolete Work concurrently with parent cancellation, so live
execution could omit an Activity-cancel command that replay then emitted. The
corrected Activity waits for genuine parent cancellation after the fence and
preserves deadline failures instead of relabeling them as cancellation. The
five queued/executing scenarios, ten repeated live unfaulted capture/replays,
three unsafe scale trials, and three protected fan-out-128 trials passed under
the race detector after the correction. This executable change leaves v2 as
valid source-pinned historical evidence, but not current-source evidence for
the present worktree; a new append-only population is required for a renewed
current-source claim.

The negative controls exposed each registered semantics boundary: premature
join after retry or terminal failure, retry double-counting in reduction, stale
generation effects after queued or executing supersession, repeated destructive
apply after ambiguous completion or caller redelivery, and lost destructive
work when an unsafe pre-effect marker was mistaken for a durable receipt.

## Inference

The four orchestration mechanisms are executable and distinguishing in both
topology shapes at the canonical development scale. Under the tested protected
protocol, fixed membership, unique reduction identity, generation/capability
fencing, stable destructive operation IDs, version preconditions, and receipt
reconciliation compose with Temporal recovery without violating their named
invariants. A Child Workflow is not required to add different business or retry
semantics for these cases.

This is not a topology-parity or performance result. There is one development
pair per mechanism combination, no 8/32/128 population estimate, and no pilot
or publication analysis. The six recovery-dynamics cases are not implemented
by this finding. No exactly-once claim follows: the protected destructive result
depends on the tested destination's atomic version/operation/receipt protocol,
while the unsafe destination repeats the physical apply.

## Responsibility boundary

Temporal supplies durable Workflow and Child Workflow procedure, Activity
redelivery and cancellation records, and replayable Event History. Application
code supplies immutable item membership, stable logical IDs, deterministic
accumulators, explicit retry ownership, generation/capability validation, and
reattachment to a still-running process after Worker loss. The destination
supplies authoritative fencing, operation/version checks, and durable receipt
reconciliation. The harness supplies exact process barriers, Worker restart,
sealed evidence, and the independent causal oracle.

The observed pass is therefore a composition result. It does not promote an
application fence, destination receipt, or process controller into a native
Temporal guarantee.

## What would change this conclusion

This finding is false if any stored run fails inventory verification or replay;
if either arm receives different Work input or Activity options; if a protected
run continues before complete membership, double-counts a contribution, accepts
obsolete authority, repeats or loses the destructive transition, or fails to
recover; or if any unsafe case ceases to expose a safety violation. A complete
recovery-dynamics or relative-cost claim based on this development root would
also exceed the evidence.
