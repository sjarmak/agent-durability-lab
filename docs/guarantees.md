# Guarantee summary

Who supplies each property, one line per cell, by architectural role. **No**
means the property is outside the cited durable-execution guarantee, not that
the implementation is defective. Click a property for the full mechanism,
evidence, and limits.

Every row below is evidenced with **Temporal**, currently the lab's most
extensively instrumented durable-execution implementation. Rows also carry a
PostgreSQL queue/lease/outbox result where that adapter has run the same case;
see each property page for the per-implementation breakdown. A row without a
PostgreSQL or other-implementation citation is a Temporal-only observation, not
a claim about durable execution in general.

| Property | Durable execution / coordinator | Application | External system |
| --- | --- | --- | --- |
| [Workflow state and replay](guarantees/workflow-state-and-replay.md) | Yes, for recorded procedure | Deterministic, replay-compatible code | Service persistence |
| [Activity delivery and completion](guarantees/activity-delivery-and-completion.md) | Redelivers; rejects a stale task token | Must fence completion by logical work-item ID | Authority-store durability |
| [External exactly-once effect](guarantees/external-effects.md) | **No** | Stable effect ID, one protocol per destination | Must dedupe, transact, or expose reconciliation state |
| [Agent process identity and ownership](guarantees/agent-process-ownership.md) | **No** | Session key, generation, process registry, start-or-attach | Store serialization and trusted discovery |
| [Vendor CLI sessions (Claude, Codex)](guarantees/vendor-cli-sessions.md) | **No**; redelivery only, no attach or fence | Supervisor outside the executor owns authority | Must enforce the fence and effect identity |
| [Cancellation and cleanup](guarantees/cancellation-and-cleanup.md) | Cancel request is durable; revocation is **no** | Atomic terminal revocation before any stop signal | OS reachability; every destination enforces revocation |
| [Sandbox lifecycle](guarantees/sandbox-lifecycle.md) | Retries the delivery; **no** provider idempotency | Stable operation key, orphan reconciliation | Provider must return the original receipt |
| [Streams and large artifacts](guarantees/streams-and-artifacts.md) | Retains accepted items and offsets | Logical output ID, reset rule, separate acknowledgement | UI, broker, or store owns cursor and retention |
| [Worker deployment versioning](guarantees/worker-versioning.md) | Routes tasks and records versions | Declares and rejects agent builds | Session registry must match the real agent |
| [Bounded recovery and topology](guarantees/bounded-recovery-and-topology.md) | Schedules durably; **no** budget or admission policy | Owns budgets, admission, poison, progress deadlines | Journals must expose physical requests and outages |
| [Operational diagnosis](guarantees/operational-diagnosis.md) | Task and execution state only | Correlate attempt, process identity, progress, age | Registry and clock integrity |
| [Fault-injection integrity](guarantees/fault-injection-integrity.md) | **No**; this is the instrument, not the system | Authenticated, pre-registered exact barriers | Trusted local controller and descriptor isolation |

Implementation coverage per property, where run:

| Property | Temporal | PostgreSQL adapter | Restate | DBOS |
| --- | --- | --- | --- | --- |
| Workflow/execution state and replay | evidenced | pending | pending | pending |
| Delivery and completion | evidenced | evidenced | pending | pending |
| External exactly-once effect | evidenced | evidenced | pending | pending |
| Bounded recovery and topology | evidenced | evidenced | pending | pending |
| Everything else on this page | evidenced | pending | pending | pending |

## Scope of this ledger

- Every row is bounded by the pinned versions, host, and destinations recorded
  on its detail page. Nothing here is a claim about durable execution in
  general.
- The lab does not claim external exactly-once effects. Neither Temporal nor
  the PostgreSQL adapter's retries alone make an effect exactly once.
- Schema validation is structural evidence only. It does not upgrade any row.
  The internal
  [coding-agent durability specification](product/coding-agent-durability-v1.md)
  and [portable protocol v1](../specs/coding-agent-durability/v1/README.md)
  translate included mechanisms into product and binding contracts.
- The deterministic
  [conformance apparatus](../benchmarks/agent-durability/conformance/README.md)
  checks schedule arithmetic, the unsafe-versus-protected distinction,
  append-only publication, and independent verdict recomputation. Its
  calibration report contains no Temporal histories, so it adds no live-recovery
  or product-binding guarantee.
- Repository coverage is a code-quality gate, not experimental evidence. The
  default product gate exercises topology correctness and replay on ordinary CI;
  four latency-sensitive profiles remain opt-in, controlled-host research.
  Deferring them neither weakens their registered bounds nor creates a
  performance claim.
- Documentation alone never upgrades an application-level property to observed.
  Each completed experiment replaces "pending" with raw evidence, its finding,
  exact versions, and what would change the conclusion.
