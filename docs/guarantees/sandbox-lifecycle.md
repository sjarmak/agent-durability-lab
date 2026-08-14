# Sandbox lifecycle

A durable child Workflow around a sandbox provider gives you lifecycle state,
not idempotent provider calls or revocable references.

Back to the [guarantee summary](../guarantees.md).

## Sandbox provider operation retry

- **Temporal:** Activity retry preserves the logical Activity and redelivers it.
  The outer Update identity is separately deduplicated.
- **Your application:** derive a stable inner operation key and atomically store
  the provider effect with its receipt.
- **Your destination:** the provider must retain and return the original
  instance, snapshot, command, or stop receipt.
- **Evidence:** [all 12 unsafe harness trials applied twice; all 12 receipt-keyed arms applied once](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).

## Attached sandbox reference authority

- **Temporal:** no. Workflow routing identity is not a revocable owner
  capability.
- **Your application:** bind commands to the current generation and capability
  and reject stale authority at the provider or destination.
- **Your destination:** every workspace mutation path must enforce the fence.
- **Evidence:** [three unsafe attached references wrote after replacement; all three fenced arms rejected the stale write](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).

## Sandbox cleanup when create status is unaccepted

- **Temporal:** child cancellation and disconnected cleanup are durable only for
  state the Workflow can name.
- **Your application:** reconcile provider resources by stable session or
  operation identity and record the cleanup disposition.
- **Your destination:** the provider must support resource lookup and
  authoritative stop or delete receipts.
- **Evidence:** [all three parent-close controls left one active instance; all three reconciled arms recorded a stop receipt and left none](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).

## Sandbox snapshot and fork workspace prefix

- **Temporal:** records the accepted snapshot reference and child initialization.
- **Your application:** define snapshot lineage and verify restored workspace
  state independently.
- **Your destination:** the provider snapshot must be immutable and consistent
  for the declared semantics.
- **Evidence:** [all six forks matched the exact pre-snapshot prefix; unsafe retries also created an unreferenced snapshot](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).
