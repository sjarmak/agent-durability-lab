# Agent Durability Lab

Durable execution systems can recover orchestration state after failures. This
repository tests whether the agent application built around that orchestration
is still correct afterwards, and records where it is not.

The lab currently holds live evidence from Temporal, a PostgreSQL queue/lease/
outbox implementation, Claude Code, Codex, and simulated destinations. See
[the cross-system benchmark](benchmarks/agent-durability/README.md) for what is
compared and [who supplies each property](docs/guarantees.md) for the
architectural roles behind any one implementation.

Every result below comes from a run set with an unsafe negative control, an
exact fault barrier, preserved raw evidence, and an independent oracle. The
unsafe control comes first on purpose. A design is only interesting here if
something breaks without it.

## What the runs show

| Boundary | Without the mechanism | With it |
| --- | --- | --- |
| [Activity retry across an external effect](docs/findings/0004-one-temporal-completion-can-hide-two-effects.md) | 18/18 trials wrote the effect twice under one Temporal completion | 18/18 wrote it once |
| [`claude -p` in a retryable Activity](docs/findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md) | 9/9 faulted trials started two Claude sessions and applied two effects | see the fenced row |
| [Claude `--resume` across redelivery](docs/findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md) | 9/9 faulted trials kept one session UUID and still applied two effects | see the fenced row |
| [Claude under a fenced supervisor](docs/findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md) | matched resume-only controls duplicated 9/9 | 15/15 passed at four boundaries |
| [Codex `exec resume`](docs/findings/0021-codex-thread-resume-is-not-turn-authority.md) | 6/6 post-effect faults duplicated despite one logical thread | 27/27 passed at eight boundaries |
| [Workflow cancellation of a detached agent](docs/findings/0006-cancellation-requires-application-revocation.md) | 6/6 Temporal-only controls mutated after the Workflow closed as canceled | 18/18 application-revoked runs accepted nothing |
| [Retried stream publisher](docs/findings/0023-workflow-stream-retries-need-output-reconstruction.md) | naive concatenation produced `ABABC` on every post-flush retry | reset-at-marker reconstructed `ABC` 9/9 |

[All findings, one line each →](FINDINGS.md)  ·  [Who supplies which guarantee →](docs/guarantees.md)

## Do you need any of this?

Three questions, from
[*Engineering Reliable Coding Agents*](https://github.com/sjarmak/engineering-reliable-coding-agents):

1. Does valuable state remain exposed if the process dies mid-flight?
2. Does the workflow wait for external events?
3. Does it perform irreversible external effects?

If your answer to each of these is "no", that means you probably do not need durable execution. Use a timer and a
lock, and read the source of record again on each run. One yes means some
coordination fact cannot be reconstructed from the current source of record, and
this repository is about which of those facts the engine will own and which stay
in your code.

## The triad, without running anything

Every experiment ships three arms against the same exact fault: an unfaulted
run, an unsafe control, and a protected run. This is the output of
`./cookbooks/coding-agents/quickstart.sh`, which needs Go and no credentials:

```text
FIRST TRUSTWORTHY RECOVERY

Tool effect commits before Activity completion
Fault: The exact codex-tool-effect-committed barrier fires before Activity completion, then the Worker is replaced.

UNFAULTED  valid-pass  1 physical effect
  history: .../codex-direct-hermetic-fenced-20260812-v12.tar.gz :: codex-direct-fenced-start-or-attach-unfaulted-trial-1/workflow-history.json
UNSAFE     valid-fail  2 physical effects
  history: .../codex-direct-hermetic-unsafe-20260812-v12.tar.gz :: codex-direct-unsafe-fresh-tool-effect-before-activity-completion-trial-1/workflow-history.json
PROTECTED  valid-pass  1 physical effect
  history: .../codex-direct-hermetic-fenced-20260812-v12.tar.gz :: codex-direct-fenced-start-or-attach-tool-effect-before-activity-completion-trial-1/workflow-history.json

102 histories replayed by the credential-free transport audit.

Temporal: Records the Workflow procedure and redelivers the incomplete Activity.
Application: Owns stable logical identity, generation/capability authority, and exact start-or-attach.
Destination: Accepts only the current authorized effect and preserves its receipt.
```

The difference between the failing arm and the passing arm is one branch in the
Activity. The unsafe and resume-only arms run the CLI themselves, so each
delivery is a process:

```go
// experiments/durable-vendor-sessions/codex-direct/internal/lab/activity.go:68
if input.RecoveryMode.normalized() == RecoveryModeFenced {
	return a.runFencedCodex(ctx, input, info.Attempt)
}
result, err := a.executeAttempt(ctx, input, physicalAttemptID, actorID, info.Attempt)
```

The protected arm asks a supervisor that outlives the Worker whether this
logical session already has a live owner:

```go
// experiments/durable-vendor-sessions/codex-direct/internal/lab/activity.go:201
receipt, err := client.StartOrAttach(ctx, supervisorStartRequest{
	SessionID: input.LogicalSessionID, WorkerID: a.WorkerID, AgentBuild: "codex-direct-fenced-v1",
	Attempt: temporalAttempt, LogicalTurnID: input.LogicalTurnID, LogicalEffectID: input.LogicalEffectID,
})
```

Temporal behaves identically in both arms. The effect counts differ.

To browse the same verified triad with normalized timelines beside native
history, raw evidence, authority, effects, and provenance:

```bash
./cookbooks/coding-agents/explore.sh
```

![Recovery evidence explorer](docs/assets/recovery-evidence-explorer.png)

## Start from your problem

- **Running Claude Code or Codex under Temporal.** Read
  [0010](docs/findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md),
  [0019](docs/findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md),
  and [0020](docs/findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md),
  then apply
  [03-external-cli-ownership](cookbooks/coding-agents/03-external-cli-ownership/README.md).
- **An Activity writes to an API, database, Git, or a broker.** Read
  [0004](docs/findings/0004-one-temporal-completion-can-hide-two-effects.md),
  then apply
  [02-effect-safe-tools](cookbooks/coding-agents/02-effect-safe-tools/README.md).
- **You cancel a Workflow and the agent keeps working.** Read
  [0006](docs/findings/0006-cancellation-requires-application-revocation.md),
  then apply
  [04-cancellation-and-cleanup](cookbooks/coding-agents/04-cancellation-and-cleanup/README.md).
- **Streaming agent output to a UI that must survive a retry.** Read
  [0023](docs/findings/0023-workflow-stream-retries-need-output-reconstruction.md)
  and [0024](docs/findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md).
- **Recovery works but costs too much under load.** Read
  [0013](docs/findings/0013-application-policy-equalizes-safety-not-recovery-cost.md),
  then apply
  [06-bounded-recovery](cookbooks/coding-agents/06-bounded-recovery/README.md).
- **You want to reproduce something before believing it.** Run
  [`quickstart.sh`](cookbooks/coding-agents/quickstart.sh), then read the
  [experiment methodology](docs/experiment-methodology.md).

The applied path through all six patterns is
[Fault-Tested Durability Patterns for Coding Agents](cookbooks/coding-agents/README.md),
with [failure-first tutorials](cookbooks/coding-agents/tutorials/README.md) on how
to read the triad. A pinned Codespaces and Dev Containers workspace lives in
[`.devcontainer/`](.devcontainer/README.md); its credential-free check is
`./cookbooks/coding-agents/dev-smoke.sh`. A local
[Temporal Code Exchange submission preview](docs/product/code-exchange-submission.md)
records the current packaging fields; it has not been submitted.

## Evidence standard

Every experiment begins with a written contract stating:

- an application-level safety or liveness invariant;
- the exact failure boundary and the component being disrupted;
- a machine-checkable success and failure oracle;
- the logical, ownership, delivery, process, effect, and artifact identities;
- the responsibility split among the durable system, application, and external
  destination; and
- what would narrow or overturn the conclusion.

A qualifying experiment includes an unsafe negative control capable of violating
the invariant, injects faults at named barriers instead of timing guesses,
preserves raw evidence, and repeats concurrency-sensitive trials. Harness
mistakes and superseded runs stay visible. Full protocol:
[experiment methodology](docs/experiment-methodology.md).

## Architectural boundary

The lab's evidence spans two durable-coordinator implementations (Temporal,
PostgreSQL) and two external agent runtimes (Claude Code, Codex), but the
boundary below is implementation-neutral:

- The durable execution system records and recovers procedure: ordering,
  retries, waits, cancellation requests, and accepted completion.
- The application owns logical operation identity, current ownership authority,
  lifecycle policy, bounded recovery, and the links between executions and
  artifacts.
- The destination accepting an external mutation enforces the relevant
  idempotency key, transaction, fence, conditional publication, or
  reconciliation protocol.

An Activity attempt is a delivery attempt, not durable agent identity. A single
recorded completion is not proof of one external effect, and transcript resume
is not proof that only one live turn or workspace writer exists. The lab does
not claim generic exactly-once effects.

[The architecture hypothesis](docs/architecture.md) describes the current
component boundaries. Lasting choices are in [`docs/decisions/`](docs/decisions/),
beginning with
[evidence before shared abstraction](docs/decisions/0001-evidence-before-abstraction.md)
and the
[procedure/authority/effect boundary](docs/decisions/0002-separate-procedure-authority-and-effects.md).
[ADR 0004](docs/decisions/0004-protect-the-unattributed-column.md) commits the
project to keeping the "No" cells, the unfavorable measurements, and the
unsafe-control-first ordering as the repository gets more polished.

## Research program

The queue is organized around five questions:

1. **Execution identity and authority.** How does one logical operation retain a
   stable identity across retries while obsolete executors lose the authority to
   write, complete, acknowledge, or stop current work?
2. **External effects and publication.** What happens when an effect commits but
   its acknowledgement or Activity completion is lost, and which destination
   protocols can deduplicate, fence, or reconcile the ambiguity?
3. **Lifecycle recovery.** How should an application recover processes, vendor
   sessions, cancellation, streams, artifacts, and versioned deployments when
   durable state and external reality temporarily disagree?
4. **Bounded recovery under load.** How do retry budgets, admission control,
   poison isolation, backpressure, progress detection, and orchestration
   topology affect safety, liveness, and recovery cost?
5. **Cross-system comparison.** Which guarantees come from a durable-execution
   system, which come from application policy, and which require cooperation
   from the destination? Comparisons hold the workload, failure schedule,
   evidence, and oracle fixed while allowing idiomatic system adapters.

Open questions are in [research questions](docs/research-questions.md); dated
status snapshots are in [`docs/progress/`](docs/progress/README.md).

## Repository guide

- [`FINDINGS.md`](FINDINGS.md): every supported claim, one line each.
- [`docs/guarantees.md`](docs/guarantees.md): who supplies each property.
- [`docs/findings/`](docs/findings/): the full findings.
- [`docs/plans/`](docs/plans/): active experiment and benchmark designs.
- [`experiments/`](experiments/): contracts, harnesses, oracles, and
  append-only evidence.
- [`benchmarks/agent-durability/`](benchmarks/agent-durability/): mechanism-
  neutral cross-system contracts and adapters.
- [`internal/`](internal/): mechanisms shared only after experiments establish a
  real common boundary.

Each experiment README answers the local question, invariant, failure boundary,
oracle, run command, evidence location, responsibility split, and what would
falsify it.

## Build and verify

The lab requires Go and, for live Temporal experiments, a compatible Temporal
CLI. Exact versions and dependencies are recorded with each experiment and run
set.

```bash
make build
go test -race ./...
make coverage
```

Some tests start local services and send real signals to subprocesses. Read the
relevant experiment README before generating evidence; evidence commands require
explicit output roots and never overwrite an existing run directory.

Contributions are gated on evidence rather than style. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md). A polished demo,
a healthy Workflow, or a single passing run is not sufficient evidence.

## License

MIT. See [LICENSE](LICENSE).
