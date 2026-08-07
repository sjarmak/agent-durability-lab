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

## Run it

Prerequisites are Go 1.25.12 or newer and Temporal CLI 1.8.0 or newer (bundling
Server 1.31.2 or newer for the current Standalone Activity-era APIs).

```bash
make build
./bin/worker-death-experiment --mode all --run-id local-trial
./bin/worker-death-experiment --scenario launch-gap --arm all --run-id launch-gap-local
./bin/activity-completion-identity-experiment --arm all --trials 3 --run-id completion-local
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
- `experiments/worker-death/`: the first experiment, offline oracle, live harness,
  and preserved evidence.
- `experiments/activity-completion-identity/`: task-token versus logical-ID
  completion experiment, durable attempt fence, and preserved evidence.
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
