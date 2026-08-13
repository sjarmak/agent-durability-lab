# Worker death with a surviving agent

## Question

When a Worker dies but the independently launched agent remains alive, does an
Activity retry attach to the logical operation, create a competing writer, or
explicitly replace and fence the old writer?

## Invariant and controls

The safe invariant is:

> One logical operation has no more than one authorized writer at a time and
> eventually exposes one accepted terminal outcome.

The unsafe arm is a negative control. It is valid only if attempt 2 blindly
launches another executor and the synthetic destination accepts both effects.
The command exits successfully for this arm when the safety violation is
reproduced; `verdict.json` still records `invariant_satisfied: false`.

The reattachment arm uses an atomic stable session lookup. Attempt 2 receives the
generation-1 lease and launches no child.

The fenced arm makes replacement explicit on attempt 2. The application store
advances the generation and issues a different owner token. Generation 2 finishes
first; generation 1 is then released and both its effect and completion must fail
with `stale_owner`.

## Exact failure boundary

Two independent barriers must be observed before the kill:

1. `before-effect/1`: the child has registered PID/start identity and emitted
   externally observable progress but has not attempted the canonical effect.
2. `activity-before-first-heartbeat/1`: Activity attempt 1 has resolved/launched
   the session but has not recorded its first heartbeat.

The controller then sends `SIGKILL` to Worker 1's exact PID and waits for the
signaled process status. It rereads `/proc/<pid>/stat` and the Linux boot ID to
prove that the registered child (not a reused PID) is still alive. Only Temporal's
heartbeat timeout causes attempt 2; the harness never invokes the Activity
directly.

Timeouts bound a hung trial. They do not open or select the failure window.

## Run

```bash
make build
./bin/worker-death-experiment --mode unsafe --run-id control-1
./bin/worker-death-experiment --mode reattach --run-id reattach-1
./bin/worker-death-experiment --mode fenced --run-id fenced-1
```

`--mode all` runs the three arms sequentially and appends the mode to the run ID.
The command requires a locally installed `temporal` CLI unless `--temporal` is
provided.

The adjacent launch/registration-gap scenario has its own contract in
[launch-registration-gap.md](launch-registration-gap.md). Run its negative
control and conditional fenced recovery with:

```bash
./bin/worker-death-experiment --scenario launch-gap --arm all --run-id launch-gap-local
```

The control records a PID-less `launch_pending` phantom and then cancels the
Workflow. The recovery arm replaces only that pending claim; the store tests
also prove that the same policy attaches after a process reaches `running`,
rejects out-of-order older replacement attempts, and rejects mutation before
process registration.

The final preserved launch-gap evidence uses agent protocol `worker-death-v3`:

- [control](evidence/launch-gap-20260806-v3-control/verdict.json): attempt 2
  attaches to generation 1 with no PID, effect, or outcome; the harness records
  the phantom before cancellation.
- [fenced recovery](evidence/launch-gap-20260806-v3-fenced-recovery/verdict.json):
  generation 2 registers one process and produces one matching effect/outcome.

The v1/v2 launch-gap directories remain unchanged. They reproduce the same
application states but predate the monotonic-attempt guard, store-enforced
registration gate, and full expected barrier-identity validation added after
independent review; they are not the basis for the final mechanism claim.

The later post-`exec`, pre-registration scenario is specified in
[post-exec-registration-gap.md](post-exec-registration-gap.md). It compares
reattaching to a child proven alive through an independent discovery barrier
with fenced replacement and stale-registration cleanup:

```bash
./bin/worker-death-experiment \
  --scenario post-exec-gap --arm all --trials 3 \
  --run-id post-exec-local
```

The final `post-exec-gap-20260806-v3-*` matrix preserves three trials per arm,
including a standalone `pre-kill-state.json` captured immediately before each
Worker kill. Representative [attach](evidence/post-exec-gap-20260806-v3-attach-control-trial-1/verdict.json)
and [replacement](evidence/post-exec-gap-20260806-v3-fenced-replacement-trial-1/verdict.json)
verdicts are valid. V1 has an obsolete protocol label and v2 lacks the standalone
pre-kill snapshot; both remain preserved but are excluded from the final claim.

## Evidence and oracle

Each run preserves:

```text
evidence/<run-id>/
  manifest.json
  events.jsonl
  application-state.json
  pre-kill-state.json       # post-exec registration-gap scenario
  verdict.json
  temporal-history.json
  temporal-server.log
  workers/
  sessions/
```

The local `application.db` and `temporal.db` are useful for forensic reruns but
are ignored by Git; the portable JSON evidence and logs are the checked evidence.
Owner credentials never enter JSONL; only SHA-256 token digests do.

The verifier is independent of the orchestration sequence. Its hand-authored
fixtures prove that it rejects an unsafe control that fails to duplicate, a
reattachment that creates another executor, and stale rejections in the wrong
order.

## Preserved milestone run

The final 2026-08-06 v3 evidence was produced with Go 1.25.12, Go SDK
1.47.0, Temporal CLI 1.8.0, and Server 1.31.2. Its oracle also proves that the
Workflow return value equals the application store's accepted outcome:

- [unsafe control](evidence/milestone1-20260806-v3-unsafe/verdict.json): two
  executors, two accepted effects, one terminal winner.
- [stable reattachment](evidence/milestone1-20260806-v3-reattach/verdict.json): two
  Activity attempts, one executor, one effect, generation-1 outcome.
- [fenced replacement](evidence/milestone1-20260806-v3-fenced/verdict.json):
  generation-2 outcome at event 15, generation-1 effect rejection at event 16,
  and generation-1 completion rejection at event 18.

The original and v2 directories are retained unchanged. Review found that the
original verdicts did not compare the Temporal return value with the accepted
application outcome; v2 added that assertion. Security review then required an
isolated child environment and a patched Go toolchain, producing v3 rather than
rewriting either earlier evidence set.

The reattachment history is also the replay fixture. The current Workflow
replays it successfully; a test-only Workflow that adds an unrecorded timer is
rejected with SDK nondeterminism code `TMPRL1100`.

## Responsibility split

Temporal provides the Event History, Activity heartbeat timeout, retry policy,
and attempt-2 task delivery. The application provides the stable session lookup,
process registry, explicit replacement, generation/token validation, accepted
outcome transition, barrier journal, and oracle. Linux provides the process
survival and PID/start-time evidence. The synthetic destination participates in
fencing by validating the token through the application store.

## Limits and falsifier

This is single-host evidence. It does not prove that another host can address the
surviving child, that a wedged child should be replaced automatically, that
arbitrary Git/API destinations enforce the fence, or that cancellation reaches
the child after Worker death.

The pre-`exec` and post-`exec` sides of the launch/registration window are now
captured by [finding 0002](../../docs/findings/0002-launch-decision-is-not-process-liveness.md)
and [finding 0005](../../docs/findings/0005-launch-pending-does-not-identify-process-reality.md).
Together they show that identical `launch_pending` store state can represent no
process or a live unregistered process. Cross-host discovery and uncooperative
cleanup remain unresolved.

The barrier service is unauthenticated and bound to loopback. The launch-gap
harness validates the full expected session, generation, owner hash, actor, and
arrival ID before injecting `SIGKILL`, but a trusted same-user process could still
read local state and forge that identity. It is a laboratory coordination
mechanism, not a production process-control API. Raw owner tokens
exist only in the mode-0600 application database and a mode-0600 launch request,
which the simulator removes immediately after decoding. Portable evidence and
logs contain token hashes.

The child currently runs under the same OS user and receives the store path plus
its raw generation token. The fence therefore proves fail-closed behavior for a
cooperative executor using the store API, not containment of a compromised local
process that edits the database directly. A production design would put the
authoritative mutation behind an authenticated broker or stronger sandbox and
issue revocable, generation-scoped destination credentials. Detached children
inherit no ambient Worker environment variables.

The conclusion is falsified if a safe reattachment creates a second child,
generation 1 dies with Worker 1, an obsolete generation's effect/outcome is
accepted after replacement, the accepted result changes after the stale attempt,
or replay accepts the deliberately incompatible Workflow.
