# Agent Durability Lab

This repository asks a stricter question than whether Temporal recovers:

> After Temporal recovers, is the overall agent application still correct?

It is a single-machine engineering research lab for long-running agent processes,
external effects, ownership, cancellation, streams, artifacts, and deployment
changes. Experiments start with an invariant and a negative control, inject a
fault at an exact barrier, and preserve machine-checkable evidence.

## First result: Worker death with a surviving agent

The first experiment runs a real Temporal dev server, two Worker processes, and
detached agent simulators. Worker 1 is killed with `SIGKILL` after its child has
reported progress and immediately before the Activity's first heartbeat. Temporal
times out the Activity task and dispatches attempt 2 to Worker 2.

| Arm | Executors | Accepted effects | Outcome | Safety result |
| --- | ---: | ---: | --- | --- |
| Unsafe retry | 2 | 2 | one winner | invariant violated as expected |
| Stable reattachment | 1 | 1 | generation 1 | invariant satisfied |
| Fenced replacement | 2 (explicit replacement) | 1 | generation 2 | old effect and completion rejected |

The result separates three mechanisms:

- Temporal durably redelivered the Activity after the Worker stopped heartbeating.
- The application session registry made retry attach to the surviving child.
- The application generation/token check rejected the obsolete child's authority.

Temporal's stale Activity task-token validation is not the same as application
writer fencing. The child never possesses a Temporal task token; without the
application check it can still mutate an external destination.

See [the experiment contract](experiments/worker-death/README.md),
[the first finding](docs/findings/0001-worker-death-surviving-agent.md), and
[the guarantee ledger](docs/guarantees.md).

## Second result: a launch claim is not a live process

The next boundary kills Worker 1 after `executor_launch_decided` is durable but
before the Activity calls the process launcher. Blind retry reattachment finds
the session yet can never observe an outcome: generation 1 is `launch_pending`
with no PID. Temporal is retrying correctly while the application is stuck.

The minimal recovery mechanism records `launch_pending` separately from
`running`. Attempt 2 conditionally replaces only the pending claim under fenced
generation 2. The final v3 control/recovery pair, two earlier preserved pairs,
and two additional final-protocol live trials per arm reproduce the distinction.

See [the exact contract](experiments/worker-death/launch-registration-gap.md) and
[finding 0002](docs/findings/0002-launch-decision-is-not-process-liveness.md).

## Third result: logical Activity ID is not attempt identity

The asynchronous-completion experiment lets attempt 1 time out, observes
attempt 2 start, and then submits a completion attributed to obsolete attempt 1.
Across three live trials per arm, attempt 1's task token was rejected, while
`CompleteActivityByID` accepted the stale result and completed the current
logical Activity. An application-owned opaque capability fence rejected the
stale caller before the by-ID RPC.

See [the experiment](experiments/activity-completion-identity/README.md) and
[finding 0003](docs/findings/0003-activity-id-completion-is-not-attempt-scoped.md).

## Fourth result: one Temporal completion can hide two external effects

The external-effect experiment kills Worker 1 after a destination confirms its
mutation and before the Activity returns. Temporal times out attempt 1, retries
on Worker 2, and records one Activity completion in every run. In all 18 unsafe
trials the destination nevertheless contains two physical effects.

Six destination-specific protected arms each left one effect across three live
trials: HTTP idempotency key, correlation lookup before retry, transactional
unique key, Git marker reconciliation, simulated message-destination ID deduplication, and a
content-addressed artifact plus stable reference. These are destination and
application mechanisms, not a Temporal exactly-once guarantee.

See [the experiment contract](experiments/external-effects/README.md) and
[finding 0004](docs/findings/0004-one-temporal-completion-can-hide-two-effects.md).

## Fifth result: pending launch state cannot reveal process reality

The post-`exec`, pre-registration experiment kills Worker 1 only after both the
child and Activity reach independent barriers. At that instant, the application
store still shows generation 1 as `launch_pending` with PID 0, while the
preserved boundary snapshot proves a distinct child PID/start identity is alive.

Across three trials per arm, a discovery-backed attach reused that exact child
without a competitor. Explicit replacement advanced to generation 2; the old
child remained alive, then its delayed registration was rejected with its exact
identity and it exited. Combined with the earlier pre-`exec` phantom, this proves
the same durable state can describe two different external realities.

See [the experiment](experiments/worker-death/post-exec-registration-gap.md) and
[finding 0005](docs/findings/0005-launch-pending-does-not-identify-process-reality.md).

## Sixth result: cancellation is not detached-process revocation

Across six Temporal-only controls, the Workflow closed as canceled under both
Activity wait policies, yet its detached agent subsequently committed an effect
and outcome. Eighteen safe runs added an application-store terminal revocation
before exact process control; no post-cancel effect or outcome was accepted.

The safe matrix covered a healthy Worker, Worker 1 `SIGKILL` followed by Worker
2 cleanup, and a frozen leader/tool-child process group. Every safe run recorded
revocation, delivery, acknowledgement, and both process dispositions
separately. `WaitForCancellation=true` changed the Event History acknowledgement
shape, not the application authority result.

See [the cancellation experiment](experiments/cancellation/README.md) and
[finding 0006](docs/findings/0006-cancellation-requires-application-revocation.md).

## Run it

Prerequisites are Go 1.25.12 or newer and Temporal CLI 1.8.0 or newer (bundling
Server 1.31.2 or newer for the current Standalone Activity-era APIs).

```bash
make build
./bin/worker-death-experiment --mode all --run-id local-trial
./bin/worker-death-experiment --scenario launch-gap --arm all --run-id launch-gap-local
./bin/worker-death-experiment --scenario post-exec-gap --arm all --trials 3 --run-id post-exec-local
./bin/activity-completion-identity-experiment --arm all --trials 3 --run-id completion-local
./bin/external-effect-experiment --destination all --mode all --trials 3 --run-id effects-local
./bin/cancellation-experiment --scenario all --wait-policy both --trials 3 --run-id cancellation-local
```

Each run creates a new directory in its experiment's `evidence/` directory.
Existing run directories are never overwritten.

The default verification target includes unit, integration, process, replay, and
live Temporal tests. It starts local dev servers and sends real `SIGKILL`s to
Worker processes:

```bash
make test
```

Use `make coverage` for the enforced 80% aggregate coverage gate over the core
mechanism packages in `internal/`. Command entry points and the live experiment
harness are exercised by `make test` but excluded from that percentage because
their important behavior occurs in separately built subprocesses. The
Linux-specific process tests skip on other operating systems; the state-machine
and Workflow tests remain portable.

## Repository map

- `docs/`: questions, architecture hypotheses, methodology, guarantee ledger,
  decisions, and supported findings.
- [Gas City field lessons and research plan](docs/briefings/gas-city-field-lessons-and-research-plan.md):
  shareable synthesis of the field evidence, controlled lab findings, product
  implications, inherited benchmark method, book framework, and remaining work.
- `benchmarks/agent-durability/`: the machine-checked, mechanism-neutral contract
  for the planned Temporal/Restate/DBOS/PostgreSQL comparison.
- `experiments/worker-death/`: the first experiment, offline oracle, live harness,
  and preserved evidence.
- `experiments/activity-completion-identity/`: task-token versus logical-ID
  completion experiment, durable attempt fence, and preserved evidence.
- `experiments/external-effects/`: six destination classes at the
  effect-success/completion-loss boundary, with unsafe controls and preserved
  repeated evidence.
- `experiments/cancellation/`: Temporal-only controls and application-revoked
  cancellation across Worker death, wait policies, and frozen process trees.
- `internal/workstore/`: atomic session, generation, effect, outcome, and event
  state used at the application correctness boundary.
- `internal/failureinject/`: named HTTP barriers; timeouts guard deadlocks but do
  not choose failure timing.
- `internal/agentsim/` and `internal/agentprocess/`: deterministic agent behavior
  and detached OS-process launching.
- `internal/temporalagent/`: deterministic Workflow and retrying Activity.
- `cmd/worker/` and `cmd/agent-simulator/`: process entry points.

Read [AGENTS.md](AGENTS.md) before adding a mechanism or claim. A polished demo,
a healthy Workflow, or a single passing run is not sufficient evidence.
