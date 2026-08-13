# Agent Durability Lab

This repository investigates a stricter question than whether a durable
execution system recovers:

> After recovery, is the overall agent application still correct?

The lab studies long-running agent work that crosses several independently
durable systems: an orchestrator, application-owned state, operating-system or
vendor processes, and external destinations such as APIs, databases, Git,
message systems, and artifact stores. Recovery in one layer does not by itself
establish correct ownership, effects, cancellation, or completion across the
others.

This is an evidence lab, not a general agent framework or a collection of
successful Workflow demos. Its purpose is to identify failure boundaries,
reproduce them under controlled faults, and establish the smallest mechanism
that survives them.

## Start with the product surface

[Fault-Tested Durability Patterns for Coding Agents](cookbooks/coding-agents/README.md)
is the applied path through the lab. It starts with the credential-free
[`quickstart.sh`](cookbooks/coding-agents/quickstart.sh),
then connects six implementation patterns to their exact unsafe controls,
protected outcomes, native histories, and bounded findings. The product layer
is read-only and does not replace the independent evidence oracles.

A pinned Codespaces/Dev Containers workspace is available in
[`.devcontainer/`](.devcontainer/README.md). Its CI-shaped, credential-free check is:

```bash
./cookbooks/coding-agents/dev-smoke.sh
```

Open the same verified unsafe-versus-protected triad as a read-only evidence walkthrough:

```bash
./cookbooks/coding-agents/explore.sh
```

![Recovery evidence explorer](docs/assets/recovery-evidence-explorer.png)

The [failure-first tutorials](cookbooks/coding-agents/tutorials/README.md) explain how to read
the triad, apply the universal identity/authority/effect pattern, and keep normalized
presentation separate from native history and the independent oracle. A local
[Temporal Code Exchange submission preview](docs/product/code-exchange-submission.md) records
the current packaging fields; it has not been submitted.

## Research program

The intended work is organized around five related questions:

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

The [research questions](docs/research-questions.md) describe the investigation
queue. Dated completion state and result summaries live under
[`docs/progress/`](docs/progress/README.md), rather than in this overview.

## Evidence standard

Every experiment begins with a written contract that states:

- an application-level safety or liveness invariant;
- the exact failure boundary and the component being disrupted;
- a machine-checkable success/failure oracle;
- the logical, ownership, delivery, process, effect, and artifact identities;
- the responsibility split among the durable system, application, and external
  destination; and
- an observable falsifier that would narrow or overturn the conclusion.

A qualifying experiment includes an unsafe negative control capable of
violating the invariant, injects faults at named barriers instead of timing
guesses, preserves raw evidence, and repeats concurrency-sensitive trials.
Harness mistakes and superseded runs remain visible. The full protocol is in
[the experiment methodology](docs/experiment-methodology.md).

## Architectural boundary

The working architecture separates three kinds of responsibility:

- The durable execution system records and recovers procedure: ordering,
  retries, waits, cancellation requests, and accepted completion.
- The application owns logical operation identity, current ownership authority,
  lifecycle policy, bounded recovery, and the links between executions and
  artifacts.
- The destination accepting an external mutation must enforce the relevant
  idempotency key, transaction, fence, conditional publication, or
  reconciliation protocol.

An Activity attempt is a delivery attempt, not durable agent identity. A single
recorded completion is not proof of one external effect, and transcript resume
is not proof that only one live turn or workspace writer exists. The lab does
not claim generic exactly-once effects.

[The architecture hypothesis](docs/architecture.md) describes the current
component boundaries. Lasting choices are recorded in
[`docs/decisions/`](docs/decisions/), beginning with
[evidence before shared abstraction](docs/decisions/0001-evidence-before-abstraction.md)
and the
[procedure/authority/effect boundary](docs/decisions/0002-separate-procedure-authority-and-effects.md).

## Repository guide

- [`docs/progress/`](docs/progress/README.md) contains dated research-status
  snapshots.
- [`docs/findings/`](docs/findings/) contains supported, falsifiable findings.
- [The guarantee ledger](docs/guarantees.md) attributes each property to
  Temporal, application code, or an external system and links it to evidence.
- [`docs/plans/`](docs/plans/) contains active experiment and benchmark designs.
- [`experiments/`](experiments/) contains experiment contracts, harnesses,
  oracles, and append-only evidence bundles.
- [`benchmarks/agent-durability/`](benchmarks/agent-durability/) contains the
  mechanism-neutral cross-system contracts and adapters.
- [`internal/`](internal/) contains mechanisms shared only after the experiments
  establish a genuine common boundary.

Each experiment README answers the local question, invariant, failure boundary,
oracle, run command, evidence location, responsibility split, and falsifier.

## Build and verify

The lab requires Go and, for live Temporal experiments, a compatible Temporal
CLI. Exact versions and additional dependencies are recorded with each
experiment and evidence population.

```bash
make build
go test -race ./...
make coverage
```

Some tests start local services and send real signals to subprocesses. See the
relevant experiment README before generating evidence; evidence commands require
explicit output roots and never overwrite an existing run directory.

Read [AGENTS.md](AGENTS.md) before adding a mechanism or claim. A polished demo,
a healthy Workflow, or a single passing run is not sufficient evidence.
