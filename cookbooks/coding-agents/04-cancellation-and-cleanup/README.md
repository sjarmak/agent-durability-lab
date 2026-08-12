# Cancellation and cleanup

This cookbook is a thin executable guide to
[Finding 0006](../../../docs/findings/0006-cancellation-requires-application-revocation.md)
and the admitted
[cancellation experiment](../../../experiments/cancellation/README.md). It does
not copy the experiment or reinterpret Workflow cancellation as an authority
fence.

## Question

When a coding-agent Workflow is canceled, how do we prove that the logical
agent can no longer mutate state, that the exact executor received a stop, and
that its complete process tree is gone?

## Invariant

After application revocation commits for a logical session, that session must
accept no later registration, progress, effect, outcome, or replacement
generation. Temporal `WaitForCancellation` controls whether Workflow procedure
waits for Activity cancellation acknowledgement; neither setting revokes an
agent process or a destination credential.

Use five distinct durable observations:

1. Workflow cancellation was requested.
2. Application cancellation committed and revoked the current generation.
3. A stop was delivered to the exact session, generation, owner digest, PID,
   process-start identity, and process group.
4. The matching executor acknowledgement was received, or its absence was
   recorded.
5. The exact leader and every recorded descendant left the process tree, or
   their disposition remains explicitly unresolved.

## Failure boundary

The exact barrier is after the detached executor and its tool child are
registered and blocked before the first effect. The protected procedure uses a
disconnected Workflow context to run cleanup after the original Workflow
context is canceled:

1. Atomically commit terminal cancellation and revoke the active generation.
2. Reject all later work at the application store and destination boundary.
3. Resolve the durable target recorded by that cancellation receipt.
4. Deliver cooperative stop to that exact target, then record delivery.
5. Record acknowledgement only from the matching target.
6. Escalate and verify leader plus descendant exit without retargeting a new
   generation.

Never implement this as “cancel whatever PID is current.” A delayed stale stop
for generation 1 must not reach generation 2. First durable terminal transition
wins when completion and cancellation race.

## Oracle

`run.sh check` independently reads the admitted matrix without changing it. It
requires exactly 24 v2 runs: three trials of the Temporal-only control,
healthy-safe, Worker-death-safe, and frozen-safe scenarios under both wait
policies. Every run must retain its manifest, verdict, application snapshot,
barrier snapshot, event journal, and Temporal history. The audit derives cancel-event counts
from the raw history, verifies revocation-before-delivery ordering, binds the stop target to
the acknowledgement, requires both recorded processes to be gone, and seals the complete
portable v2 tree. These legacy histories are inspected, not replayed by this cookbook.

The six Temporal-only controls are valid only when cancellation procedure
completes while the detached child subsequently commits one effect. The 18
safe runs are valid only when durable application revocation exists and no
effect is accepted. All histories contain one cancel request; the preserved
population contains zero Activity-canceled events for `false` and one for
`true`. That observed count is not a universal absence guarantee for `false`.

`run.sh mechanisms` executes the work-store terminal-order tests and the Linux
process controls in `internal/workstore` and `internal/agentprocess`, including
leader-only, full-tree, and stale-stop cases.

## Fresh-checkout run

From the repository root on Linux with Go installed:

```bash
./cookbooks/coding-agents/04-cancellation-and-cleanup/run.sh all
```

Run the existing live Temporal matrix only when the pinned Temporal CLI and
local process-control prerequisites are available:

```bash
make build
./bin/cancellation-experiment --scenario all --wait-policy both --trials 3 \
  --run-id <new-append-only-run-id>
```

Do not reuse an evidence run ID or replace the admitted v2 directories.

## Evidence

The admitted evidence is the exact set of
`experiments/cancellation/evidence/cancellation-20260807-v2-*` directories.
Earlier v1 directories remain correction lineage and are not the cited result.
Each final run records stable session and generation identity, owner digest,
Temporal history, Worker/process identity, event sequence, UTC timestamps,
application state, boundary state, and the independently derived verdict.

## Observed result

All six Temporal-only controls accepted one post-cancel effect and failed the
application invariant. All 18 protected runs committed cancellation, accepted
no later effect or outcome, and recorded the exact leader/tool tree gone.
Worker 2 cleaned up the surviving executor after Worker 1 death. Frozen runs
committed revocation while acknowledgement was absent, then recorded exact
delivery, acknowledgement, and exit after resume.

The evidence supports the universal order “revoke, then stop, then verify.” It
does not establish hostile multi-tenant containment, cross-host cleanup, copied
credential revocation, or a generic exactly-once destination guarantee.

## Responsibility split

- Temporal owns cancellation commands, retry/task delivery, configured wait
  procedure, disconnected cleanup scheduling, and Event History.
- The application owns the terminal transaction, generation/capability fence,
  first-commit-wins rule, stable target, delivery/acknowledgement distinction,
  and evidence journal.
- The destination owns rejection of revoked authority and durable effect
  receipts.
- Linux owns pidfd and process-group signal delivery plus observable process
  exit; stronger isolation can require a supervisor or cgroup boundary.

## Falsifier

This recipe is falsified if a protected run accepts any mutation after durable
revocation, allows replacement after terminal cancellation, infers
acknowledgement from delivery, permits a stale stop to reach a replacement,
claims cleanup while a recorded descendant survives, depends on the launching
Worker remaining alive, or cannot reconstruct its cancellation event counts from the raw
Temporal history.
