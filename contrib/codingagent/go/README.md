# Go coding-agent protocol binding

This incubating package is an immutable application-protocol kernel, not an
agent runtime. `Kernel.Apply` mechanically enforces the v1 lifecycle,
generation fencing, capability checks, operation-specific receipt subjects,
and operation replay rules. Callers own durable atomic persistence around each
returned value.

Raw owner capabilities are 256-bit bearer secrets. Store them outside Temporal
history, arguments, results, logs, and evidence; only `Capability.Digest()` is
portable. `ExportSecret` exists solely for an application-owned secret store.

The language-neutral JSON Schemas under
`specs/coding-agent-durability/v1/` remain the wire authority. This package's
record decoder rejects duplicate keys, unknown top-level fields, malformed UTC
instants, and invalid protocol metadata; schema validation still precedes it at
an external JSON boundary. Free-form identifiers must already be redacted by
the producer. The binding does not guess whether a string is a credential.

`attach` never treats a bare identity as discovery. If registration has not
already recorded the executor, use `WithDiscoveredExecutor` only after an
application-owned supervisor or provider lookup authenticates that identity.

An exact replay reads the immutable original receipt and is authorized against
the capability that committed it, including after replacement or terminal
revocation. That historical owner still cannot create a new operation.
Coordinator replays require application authentication. `request_hash` is the
caller's canonical content hash; the kernel additionally binds the
operation-specific command values. Transition timestamps and receipt allocation
metadata describe the delivery envelope, not new operation content.

The schema adapter caps a document at 4 MiB, 64 nesting levels, and 10,000
entries per collection. It rejects invalid UTF-8 and confines evidence,
history, and observation artifact paths before returning success.

Temporal supplies durable procedure and retry. The application supplies atomic
storage, coordinator authentication, ownership policy, and cancellation. The
destination supplies the declared effect guarantee or reconciliation receipt.
