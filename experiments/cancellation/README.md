# Precise cancellation across Worker and ownership failure

## Question

When a Temporal Workflow is canceled, what evidence establishes that the
logical agent operation lost authority, that the correct executor received a
stop request, and that its complete process tree is gone?

The experiment does not treat those observations as synonyms.

## Hypotheses and invariants

Temporal records Workflow and Activity cancellation procedure, but it does not
own the detached agent process or revoke its application capabilities. A
Temporal-only cancellation can therefore coexist with a live child that still
mutates application state.

The safe application invariant is:

> After logical-session cancellation commits, the canceled session accepts no
> later registration, progress, effect, outcome, or replacement generation.

Logical cancellation and executor stopping are different operations:

- **Logical cancellation** is a terminal application-store transition. It
  revokes whichever generation is active at its serialization point and
  prevents replacement.
- **Executor stop** targets one session, generation, PID, process-start
  identity, and process group. A delayed stop for generation 1 must not affect
  generation 2.

Completion and cancellation use first-durable-terminal-transition-wins. If the
outcome commits first, cancellation reports `already_completed` without
rewriting it. If cancellation commits first, the later effect/completion fails
closed. Wall-clock request timestamps do not override store transaction order.

## Observation model

Every safe run records these separately:

1. cancellation requested;
2. application cancellation committed or an existing outcome won;
3. generation authority revoked;
4. executor-stop delivery attempted;
5. cooperative acknowledgement received or absent;
6. leader and descendant process identities confirmed gone, still alive,
   unreachable, or unknown.

Logical cancellation can be correct while process disposition remains
unresolved. A single `cancelled: true` flag is not an acceptable oracle.

## Experiment matrix

| Arm | Exact boundary | Expected observation |
| --- | --- | --- |
| Temporal-only control, `WaitForCancellation=false` | child is registered and blocked before its first effect | Workflow cancels without application revocation; releasing the detached child permits a post-cancel effect/outcome |
| Temporal-only control, `WaitForCancellation=true` | same | Temporal waits for Activity cancellation acknowledgement, but the detached child retains application authority unless separately revoked |
| Healthy safe cancellation, both wait policies | healthy Worker and child blocked before effect | disconnected cleanup commits logical cancellation, cooperative stop is acknowledged, no effect/outcome is accepted, and the process tree disappears |
| Worker-death safe cancellation, both wait policies | Worker receives `SIGKILL` after child and Activity barriers; child survives | another Worker performs durable cancellation and exact process control; the child cannot mutate after revocation |
| Temporarily unreachable child | registered process group is stopped at an exact barrier before logical cancellation | revocation commits while delivery acknowledgement is absent; safety does not depend on reachability; after resume/escalation the exact tree disappears |
| Delayed stale executor stop | generation 2 owns the session while generation 1 remains alive | a generation-1 stop cannot signal generation 2; generation 2 remains authoritative and can finish |
| Cancellation wins race | child and controller contend at a named pre-effect/pre-completion boundary | cancellation serializes first; later mutation returns the cancellation error and no terminal outcome appears |
| Completion wins race | same operations in the opposite barrier order | the accepted outcome remains immutable and cancellation reports `already_completed` |
| Leader-only process control | agent leader and long-lived tool child are both proven alive | stopping only the leader leaves the descendant alive, validating the negative control |
| Process-tree control | same topology | cooperative shutdown or escalation removes both exact identities without signaling an unrelated process |

Timing-sensitive arms run at least three fresh live trials. Unit and integration
tests exercise both transaction orders and stale identities without relying on
probabilistic scheduling.

## Temporal policy comparison

The live matrix compares `WaitForCancellation=false` and `true` rather than
assuming either policy is application cancellation. Activities heartbeat so the
pinned Go SDK and Server can expose actual cancellation delivery. Histories must
show when the Workflow cancellation request, Activity cancel request,
acknowledgement, cleanup Activity, and Workflow close occurred.

Cleanup after Workflow-context cancellation uses a disconnected Workflow
context. That preserves Temporal procedure for the cleanup Activity; the cleanup
Activity still needs the application store and OS control mechanisms below.

## Process-control mechanism under test

The single-host Linux mechanism prefers:

1. cooperative `SIGTERM` to the isolated agent process group;
2. a pidfd for exact leader identity and escalation;
3. process-group signaling for descendants; and
4. confirmation using PID plus process-start identity for every recorded
   process.

The pidfd prevents leader PID reuse from retargeting a signal. It does not by
itself address descendants. Process-group behavior is therefore tested with an
agent-spawned tool child and a leader-only negative control. Cgroups remain out
of scope unless process groups cannot support a sound result on the pinned Linux
environment.

## Responsibility boundaries

- Temporal owns the Workflow cancellation request, Activity cancel command,
  retry/task delivery, Event History, and configured wait policy.
- The application owns the terminal cancellation transition, authority
  revocation, idempotency, first-commit-wins race, generation-specific stop
  target, acknowledgement, and evidence journal.
- Linux owns signal delivery, pidfd semantics, process groups, and observed
  process exit.
- A real external destination must still reject a canceled capability. The
  synthetic work store demonstrates that contract only at its own boundary.

Raw owner capabilities and control credentials must not enter portable
evidence. The loopback failure-injection service remains trusted same-user lab
infrastructure and is not part of the cancellation guarantee.

## Success, failure, and falsifiers

A safe run fails if cancellation is not durably serialized, any post-cancel
mutation or replacement is accepted, the stop target lacks complete logical and
OS identity, acknowledgement is inferred from request delivery, a replacement
is signaled by a stale stop, or a descendant survives a claimed process-tree
termination.

The Temporal-only control is invalid unless the detached child remains capable
of an application mutation after Workflow cancellation. The leader-only control
is invalid unless it proves the descendant survives leader termination.

The conclusions are narrowed or falsified if repeated pinned-version runs
produce a different Temporal history shape, cancellation correctness depends on
the Worker that launched the child remaining alive, a frozen child can mutate
after revocation, the terminal race lacks one serialized winner, pidfd/process
group targeting can reach an unrelated identity, or portable evidence cannot
reconstruct every stage above.

## Run

Build the Worker, simulator, and experiment runner, then run three fresh trials
for every scenario and Temporal wait policy:

```bash
make build
./bin/cancellation-experiment --scenario all --wait-policy both --trials 3 --run-id local-cancellation
```

`--scenario` also accepts `temporal-control`, `healthy-safe`,
`worker-death-safe`, or `frozen-safe`. `--wait-policy` accepts `false`, `true`,
or `both`. Run directories are append-only.

## Observed result

The final `cancellation-20260807-v2-*` matrix contains 24 valid runs: three
trials for four scenarios under both `WaitForCancellation` policies.

| Scenario | Trials | Accepted post-cancel effect/outcome | Durable cancellation | Exact leader/tool tree gone |
| --- | ---: | --- | --- | --- |
| Temporal-only control | 6 | 6 / 6 | No | Agent exited only after completing work |
| Healthy safe | 6 | 0 / 0 | Yes, acknowledged | Yes |
| Worker-death safe | 6 | 0 / 0 | Yes, acknowledged by surviving child | Yes, from Worker 2 cleanup |
| Frozen safe | 6 | 0 / 0 | Yes before resume | Yes, after exact resume and pending stop delivery |

Every `WaitForCancellation=false` history contains the Activity cancel request
but no Activity-canceled event; disconnected cleanup still completes. Every
`true` history contains the Activity-canceled event before cleanup. The
application invariant has the same result in both policies because it is
linearized in the work store, not inferred from the Temporal policy.

An earlier preserved development run intentionally blocked the Activity before
its first heartbeat while using `WaitForCancellation=true`. Cancellation could
not reach the Activity context; the Activity exhausted heartbeat-timeout retries
and the Workflow failed instead of closing as canceled. The final `true` arms
heartbeat while waiting, making cancellation delivery observable rather than
assuming it.

The process tests separately preserve the negative mechanism controls:

```bash
go test -race ./internal/agentprocess -run 'TestSignalLeaderOnly|TestSignalProcessTree|TestDelayedStaleStop'
go test -race ./internal/workstore -run 'TestCancellation'
```

Leader-only pidfd signaling leaves the tool child alive. Process-tree control
removes both recorded identities. A delayed generation-1 stop removes only its
old tree while generation 2 remains alive. The work-store tests exercise both
orders of the cancellation/completion terminal transition.

See [finding 0006](../../docs/findings/0006-cancellation-requires-application-revocation.md).
