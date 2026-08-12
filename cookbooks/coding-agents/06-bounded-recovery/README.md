# Bounded recovery policy

This cookbook turns the admitted recovery evidence in
[Finding 0016](../../../docs/findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md)
into a runnable policy checklist. The cross-system result in
[Finding 0013](../../../docs/findings/0013-application-policy-equalizes-safety-not-recovery-cost.md)
supports keeping the policy application-owned; it does not make the measured
costs portable between systems.

## Question

How should a coding-agent Workflow recover from redelivery, dependency outage,
backlog, overload, poison work, and silent progress without retry
amplification, a catch-up herd, resource starvation, or an immortal wedge?

## Invariant

Every admitted item has stable identity, one durable retry owner and budget,
one explicit terminal disposition, and application-owned authority. Recovery
must remain within registered request, catch-up concurrency, in-flight,
poison-attempt, and progress-deadline bounds. A declared wait is observable
state and must not be mistaken for a wedge.

The policy is mechanical:

- Keep logical item/operation identity stable across Activity attempts.
- Let one Workflow own the layered retry budget; dependency calls do not start
  independent unbounded retry loops.
- Derive deterministic jitter from stable Workflow inputs and use Workflow
  timers, never wall-clock randomness in Workflow code.
- Durably admit work, then release scarce Activity permits while a global
  outage/cohort barrier is unresolved.
- Resume catch-up under a fixed concurrency limit after restoration.
- Isolate durable stores per-item while keeping all generations of one item on
  the same fenced store.
- Quarantine poison work after its registered budget so healthy work can drain.
- Advance progress deadlines only on named meaningful progress; replace and
  fence a wedged generation before accepting new work.

## Failure boundary

The real Temporal suite exercises five exact crash boundaries plus layered
retry, an accumulated outage backlog with Worker loss during catch-up,
ready-worker overload, mixed healthy/poison work, and accepted progress before
an executor wedge. Both Direct-Activity and Child-Workflow arms run the same
per-item procedure, Activity options, queues, concurrency, authority, and
destination protocol. The only intended difference is the scheduling edge.

Faults occur at named barriers owned by the exact process identity. No timeout
sleep is used to open a failure window. Global coordination happens in Workflow
state; an Activity returns an intermediate retry disposition instead of holding
a Worker slot while waiting for the cohort.

## Oracle

`run.sh check` independently verifies the accepted
`recovery-20260811-v7` root. It requires exactly 52 run directories and exactly
15 regular artifacts per run, recomputes every publication-inventory SHA-256,
checks the frozen 26 scenario combinations in both topologies, requires a
captured replay-compatible history, and reconstructs explicit terminal item
dispositions and the five policy bounds.

The expected outcomes are binary: 32 unfaulted/protected runs pass correctness,
safety, liveness, and diagnosability; 20 unsafe runs remain valid evidence and
fail safety. `run.sh critical` then invokes the existing Go oracle and Temporal
history replayer over every stored run rather than trusting the stored verdict.

The registered protected bounds are:

| Policy | Bound |
| --- | ---: |
| Physical requests per item | 4 |
| Simultaneous catch-up requests | 2 |
| Admitted outstanding overload items | 8 |
| Poison attempts before quarantine | 3 |
| Wedge detection deadline | 5,000 ms |

## Fresh-checkout run

From the repository root with Go and the pinned module dependencies available:

```bash
./cookbooks/coding-agents/06-bounded-recovery/run.sh all
```

To create a new append-only live Temporal root, install the pinned Temporal CLI
and run:

```bash
make topology-recovery-conformance \
  EVIDENCE_ROOT=benchmarks/agent-durability/topology/evidence/<new-suite-id> \
  TEMPORAL_WORK_ROOT=/tmp/<new-work-id> \
  TEMPORAL_CLI_PATH="$(command -v temporal)"
```

Never point this command at the admitted v7 root.

## Evidence

The admitted root is
[`recovery-20260811-v7`](../../../benchmarks/agent-durability/topology/evidence/recovery-20260811-v7),
with 52 runs and 780 inventory-sealed artifacts at fan-out 32. Each run records
stable logical identity, generation/capability authority, Temporal and process
identity, exact event sequence, dependency/destination observations, UTC
timing, raw native history, replay status, and independent verdict.

V7 is source-pinned to the executable that produced it and remains admitted
historical mechanism evidence. It is not current-source evidence for the
present worktree after later supersession-only cancellation/replay hardening;
a new append-only population is required before making that stronger claim.

The v1/v2 roots are rejected partial correction evidence. V3-v6 are preserved
and superseded. V6 passed its contemporary audit but predates the final raw
metric reconstruction; the current oracle rejects it. None may be deleted or
rewritten.

## Observed result

All 32 protected/unfaulted runs passed and all 20 unsafe controls distinguished.
Every admitted item ended succeeded or quarantined, and every parent plus
actual Child Workflow history replayed. Protected layered retry stayed at 128
requests while unsafe reached 256; protected catch-up peaked at two while
unsafe reached eight; protected overload admitted eight while unsafe admitted
32; protected poison stopped at three attempts while unsafe used five; and
protected wedge detection stayed inside 5,000 ms without falsely revoking a
declared wait.

This is executable mechanism conformance at one canonical fan-out. It is not a topology performance comparison, a population estimate, or an exactly-once
claim.

## Responsibility split

- Temporal supplies durable Workflow/Child Workflow procedure, Activities,
  timers, cancellation, task redelivery, and replayable Event History.
- The application supplies stable identity, one retry owner, deterministic
  jitter inputs, exact admission, backpressure, per-item storage, quarantine,
  progress meaning, replacement, and fencing.
- Dependencies expose physical request and outage behavior; they do not own the
  Workflow budget.
- The work store and destination atomically compare authority, start or attach,
  reject stale effects, and retain receipts.
- The harness supplies exact barriers, Worker/process faults, append-only
  evidence, inventory sealing, and the independent causal oracle.

## Falsifier

This recipe is falsified if an inventory or history fails verification, the two
topologies receive different policy or inputs, any protected run exceeds a
registered bound, loses or double-accounts an item, accepts stale/duplicate
effects, falsely revokes a declared wait, fails to quarantine poison, fails to
recover after outage plus Worker loss, or leaves an item without a terminal
disposition. Any topology cost ranking or exactly-once inference from this root
also exceeds the evidence.
