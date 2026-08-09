# ADR 0002: Separate durable procedure, execution authority, and destination effects

**Date:** 2026-08-09
**Status:** accepted
**Deciders:** Agent Durability Lab maintainers

This record is retrospective: the boundary is already expressed in the lab's
experiments and guarantee ledger. The ADR makes that architectural choice and
its Gas City inputs explicit.

## Context

Gas City field work separates one work record from its executor, assigns
Temporal ownership of durable procedure while Beads owns work facts, and adds
bounded reconciliation, leases/fencing, drain-before-stop, and
destination-aware cleanup. Proposed Gas City decisions extend that direction
into scheduler-bound workers, activation contracts, convergent publication, and
application-owned admission/backpressure.

The lab's controlled failures show why these concerns cannot be collapsed:
Temporal can correctly redeliver work while a prior process, vendor session, or
external effect survives; a single accepted completion can coexist with stale
actions or duplicate physical effects. Gas City decisions are therefore design
inputs and sources of testable hypotheses, not laboratory guarantees.

### Gas City decisions considered

The following records were reviewed in the companion `gas-city` checkout at
commit `e41990960be8d799abb30110dcd56c1e510d5e63`. Their status is preserved
here rather than promoted by citation.

| Gas City ADR | Status at review | Relevance to this decision |
| --- | --- | --- |
| `docs/adr/0009-work-record-claim-lock-structured-outcome.md` | accepted | One work identity, one active claim, and an outcome bound to evidence. |
| `docs/adr/0010-scheduler-bound-ephemeral-workers.md` | proposed | Bind ready work and authority before spawning an executor. |
| `docs/adr/0012-temporal-beads-orchestration-boundary.md` | accepted | Temporal owns procedure; Beads owns work and artifact facts. |
| `docs/adr/0013-activation-contracts.md` | proposed | Bind session identity, authority, continuity, and lifecycle before mutable work. |
| `docs/adr/0016-bounded-reconciliation-and-the-dead-letter-state.md` | accepted | Bound recovery and make exhausted work explicit rather than retrying forever. |
| `docs/adr/0017-suspend-drains-in-flight-work.md` | accepted | Stopping admission is distinct from revoking or draining work already in flight. |
| `docs/adr/0019-bead-lease-and-fencing.md` | accepted | Use a lease generation/fence instead of owner presence alone; supersedes ADR-0015. |
| `docs/adr/0020-external-effects-resource-reclamation-and-safe-cleanup.md` | accepted | Make effect safety and cleanup destination- and generation-aware. |
| `docs/adr/0021-idempotent-convergence-and-fenced-publication.md` | proposed | Separate artifact preparation from the authoritative fenced publication step. |
| `docs/adr/0022-scheduling-admission-and-fair-share-capacity.md` | proposed | Keep scheduling, admission, and backpressure as application policy rather than delivery semantics. |

## Decision

The lab models three independently authoritative boundaries:

1. The durable execution system owns procedure history, ordering, durable waits,
   redelivery, cancellation requests, and the completion it accepts.
2. Application state owns stable logical identity, current generation and opaque
   capability, lifecycle policy, bounded recovery, and links from executions to
   outcomes and artifacts.
3. The destination that accepts a mutation owns effect deduplication,
   transactional or conditional publication, generation fencing, or the state
   needed for bounded reconciliation.

Activity attempt, process, PID, vendor session, task token, and recurring owner
label are delivery identities or observations. None substitutes for durable
logical authority. Every authoritative mutation must carry the stable operation
identity and current authority to the boundary that accepts it.

Gas City decisions retain their original status when used as research inputs.
The lab adopts a mechanism or cross-system abstraction only after a negative
control and independent oracle establish the claimed boundary, consistent with
[ADR 0001](0001-evidence-before-abstraction.md).

## Alternatives considered

### Temporal as the end-to-end authority

- **Pros:** One visible procedure and retry model.
- **Cons:** Temporal cannot observe or atomically govern every child process,
  worktree, provider resource, or destination mutation.
- **Why not:** The lab has observed correct Activity redelivery alongside stale
  writers and duplicate physical effects.

### Application state owns both work facts and retry procedure

- **Pros:** One application database appears to own all lifecycle state.
- **Cons:** Reimplements durable ordering, waits, retry, replay, and cancellation
  while still failing to make external effects atomic.
- **Why not:** Work facts and durable procedure have different responsibilities;
  merging them does not remove the destination boundary.

### A generic exactly-once effect wrapper

- **Pros:** A uniform Activity-facing interface.
- **Cons:** Hides material differences among atomic idempotency, unique
  transactions, compare-and-set publication, and sequential reconciliation.
- **Why not:** The required guarantee is supplied where the effect is accepted,
  and weaker destinations retain explicit ambiguity.

### Adopt Gas City decisions directly

- **Pros:** Reuses field-tested design reasoning quickly.
- **Cons:** Mixes field observation, proposed architecture, and controlled
  evidence; it could also import product-specific mechanisms into a comparative
  lab.
- **Why not:** Gas City decisions define hypotheses and failure boundaries, not
  outcomes for this repository's systems or experiments.

## Consequences

### Positive

- Claims name the layer that supplies each guarantee.
- Cross-system adapters can share invariants and oracles without pretending
  their mechanisms or costs are identical.
- Stale writers are rejected at the mutation boundary rather than inferred from
  an orchestrator, process, or owner label.
- Product field lessons can guide experiment order without bypassing evidence.

### Negative

- Protocols must carry stable identities, generations, capabilities, and
  receipts across system boundaries.
- External destinations require specific idempotency, fencing, publication, or
  reconciliation mechanisms.
- Recovery and cleanup need explicit budgets, terminal states, and independent
  observations.
- More boundaries require more negative controls and live failure experiments.

### Risks

- **Duplicated authority:** two layers may both appear to own the same phase.
  Mitigation: assign one source of truth per field and exchange links or
  receipts, not mirrored lifecycle state.
- **Status promotion by citation:** a proposed Gas City design may be described
  as established. Mitigation: retain source status and require lab evidence
  before upgrading a guarantee.
- **Generic interfaces erase destination semantics:** a shared API may imply
  stronger atomicity than a destination supplies. Mitigation: keep destination
  class and assumptions visible in contracts, evidence, and findings.

This decision must be revisited if a controlled experiment shows that one layer
can durably and atomically own procedure, executor authority, and the tested
destination mutation without the explicit cross-boundary protocol above.
