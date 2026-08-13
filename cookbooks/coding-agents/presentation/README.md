# Coding-agent durability presentation contract

This package defines the immutable data shown by the product-facing cookbook
and evidence explorer. It is a **read-only**, **non-authoritative** consumer of
**verified evidence**. It performs no filesystem IO, runs no oracle, and cannot
change a stored verdict.

The boundary is intentionally one-way:

```text
raw evidence -> independent audit -> Catalog -> explorer/tutorial
```

Every catalog keeps the user-facing question beside its invariant, failure
boundary, claim, responsibility split, and falsifier. Every scenario includes
the exact unfaulted, unsafe, and protected episodes. An episode carries logical
and execution identities, authority changes, effects, cancellation chronology,
terminal outcome, normalized events, and artifact references.

Authority is an observation in this layer. An unsafe episode may record that
application authority was absent, and historical evidence may show authority
still current at terminal Workflow completion. The view preserves that fact; it
does not manufacture a revocation to make an episode look conformant.

Normalized events are an index, not a substitute for native history. Artifact
references retain hashes, provenance, replay status, and correction lineage so
the consumer can reach the admitted source without treating the rendered view
as proof.

`DecodeJSON` is the untrusted-input boundary. It rejects oversized or malformed
JSON, duplicate keys, excessive nesting/items, unknown fields, invalid UTC
timestamps, unconfined paths, invalid hashes, and incomplete scenario triads.
It returns a validated value and never opens an artifact path. Consumers must
render all narrative fields as escaped text, never as trusted HTML, and must not
turn an artifact path into filesystem access without a separate confined-root
check.

The product direction and information architecture are defined in
[Fault-Tested Durability Patterns for Coding Agents](../../../docs/product/fault-tested-coding-agent-cookbooks.md).
