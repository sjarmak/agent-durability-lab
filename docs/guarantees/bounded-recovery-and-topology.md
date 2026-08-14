# Bounded recovery, cross-system comparison, and topology

Durable scheduling is not a recovery policy. These rows are where the lab
compares substrates and orchestration shapes while holding the workload,
failure schedule, evidence, and oracle fixed.

Back to the [guarantee summary](../guarantees.md).

## Cross-system recovery conformance

- **Temporal:** Workflow and Activity plans export replayable Event History and
  durably schedule the common procedure.
- **Your application:** the common harness fixes identities, named barriers,
  portable policy, append-only evidence, and the independent oracle. PostgreSQL
  queue and lease logic is application code.
- **Your destination:** PostgreSQL supplies transactions and row locks.
  Destinations still enforce effects and fences.
- **Evidence:** [the publication run set](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md)
  contains 540 valid matched pairs. Every unfaulted and protected execution
  passed all four outcomes and every unsafe control distinguished. All 540
  Temporal histories replayed and all 540 PostgreSQL journals were retained.
  Analysis v5 rehashed 13,503 sealed entries, confined legacy paths to that root,
  and exactly matched the preregistered schedule and run-set order.

## ABA owner-label reacquisition

- **Temporal:** no automatic application authority fence is claimed.
- **Your application:** allocate monotonic generations and opaque capabilities,
  and validate them at every registration, mutation, completion, acknowledgement,
  and stop boundary.
- **Your destination:** the accepting destination must atomically compare the
  current generation and capability.
- **Evidence:** in [30 publication pairs per probe](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md),
  both unsafe systems accepted four obsolete A/g7 actions after A/g9 became
  current; both fenced systems accepted zero. Protected median recovery was
  45.5 ms for Temporal and 1 ms for PostgreSQL on the pinned host. The fence, not
  the owner label or durability substrate, supplies safety.

## Bounded recovery dynamics

- **Temporal:** durable scheduling alone does not define a shared retry budget,
  admission limit, poison isolation, or external-agent progress policy.
- **Your application:** own and propagate budgets, bound retry and admission
  concurrency, quarantine poison work, and use durable progress deadlines and
  fenced replacement.
- **Your destination:** dependency and destination journals must expose physical
  requests, accepted effects, and exact outage and restoration transitions.
- **Evidence:** across [30 valid pairs in every recovery stratum](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md),
  both systems composed with the same bounded policies. Attempt counts, admission
  decisions, and active dependency time were generally equal; the pinned Temporal
  arm had higher queue, retry, and recovery delay. This is a single-host
  observation, not a universal product ranking.

## Direct-Activity versus Child-Workflow topology semantics

- **Temporal:** durably executed and replayed both scheduling shapes in the first
  four cases. This does not establish complete recovery or cost parity.
- **Your application:** the frozen design and
  [shared suite](../../benchmarks/agent-durability/topology/README.md) keep Work
  input and options, fixed membership, identity, retry ownership, fencing, exact
  barriers, evidence, and oracle common while changing only the extra Child
  Workflow scheduling edge.
- **Your destination:** the tested destination supplies authority comparison,
  operation and version checks, and durable receipt reconciliation.
- **Evidence:** [Finding 0015](../findings/0015-topology-semantics-controls-distinguish-with-replay.md)
  preserves 44 canonical development runs: all 26 unfaulted and protected arms
  passed all outcome dimensions, all 18 unsafe arms distinguished, and all
  histories replayed. This is mechanism conformance, not an 8/32/128 run set,
  recovery-dynamics, efficiency, or topology-parity claim.

## Direct-Activity versus Child-Workflow recovery dynamics

- **Temporal:** durably schedules, retries, times, cancels, and replays both
  shapes. It does not define the application retry budget, admission window,
  external-process identity, poison policy, progress meaning, or effect fence.
- **Your application:** one common per-item Workflow procedure owns Activity
  rounds and timers. The application records stable identity, exact admission,
  shared budgets, monotonic authority, process registration, quarantine, and
  terminal accounting.
- **Your destination:** the dependency service exposes physical requests and
  outage transitions. Per-item durable work stores and the destination enforce
  start-or-attach, fencing, and receipt safety. Fixed Worker Activity concurrency
  is eight in both arms.
- **Evidence:** [Finding 0016](../findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md)
  preserves the source-pinned v7 run set of 52 canonical fan-out-32 runs: all 32
  unfaulted and protected arms passed, all 20 unsafe arms distinguished, every
  included item reached an explicit terminal disposition, and every parent plus
  actual Child history replayed. A later supersession-only executable change
  makes v7 historical rather than current-source evidence; a fresh run set is
  required before renewing a current-source claim. Rejected partial roots and
  superseded complete v3-v6 roots remain unchanged. This is mechanism
  conformance, not an 8/32/128 run set, relative-cost, or exactly-once claim.

## Frozen topology matrix conformance

- **Temporal:** supplies the real Workflow, Child, Activity, timer, retry, and
  history behavior in the predetermined live sentinels. It does not validate the
  benchmark schedule or make fixture output empirical.
- **Your application:** the harness independently reconstructs exact 88-stratum
  and 3,520-block arithmetic, globally balanced pilot order, matched pair inputs,
  every registered causal, request, and history metric, replay status, executable
  provenance, and fail-closed controls. A versioned cohort gate prevents initial-
  outage retry loops from starving the exact backlog barrier.
- **Your destination:** hermetic agent, dependency, and destination services
  supply observable physical work and enforce their registered protocols.
- **Evidence:** [Finding 0018](../findings/0018-topology-measurement-admission-is-independent-before-pilot.md)
  preserves the accepted v7 root: 88 valid fixture pairs, four rejected invalid
  controls, and 23 valid live sentinel pairs. Every unsafe arm distinguished,
  every protected and unfaulted arm passed all four dimensions, all 46 live
  histories replayed, and a disk-only audit verified 3,798 sealed artifacts.
  Fixture histories are rejected from live evidence. The root is preliminary: it
  supports pilot readiness and is not ready to cite.
