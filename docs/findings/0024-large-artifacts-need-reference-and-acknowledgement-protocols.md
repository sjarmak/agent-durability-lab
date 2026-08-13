# Finding 0024: Large artifacts need reference and acknowledgement protocols

## Status

Observed and admitted for one pinned single-host filesystem boundary.

## Claim

Temporal can durably retry the procedure that produces a large artifact, and its
experimental External Storage surface can keep a large Activity payload out of Event
History. Neither property makes the application artifact complete. A recoverable coding
agent needs separate stable identities and durable records for content, logical reference,
and consumer acknowledgement, plus explicit reconciliation for publication orphans.

## Observed boundaries

Across three trials per mode and boundary, all 18 protected runs converged to one verified
blob/object; application runs also converged to one reference and one acknowledgement.
Unsafe controls distinguished the ambiguous windows:

- post-reference redelivery retained two references in 3/3 unsafe trials;
- post-acknowledgement redelivery retained two acknowledgements in 3/3 unsafe trials;
- post-StorageDriver redelivery retained two payload objects in 3/3 unsafe trials; and
- post-blob and pending-reference retries exposed orphans that required explicit
  reconciliation, while reachable content was retained.

The Activity-completed boundary passed in both modes because Temporal had recorded the
producer result before the acknowledgement Activity began. That is a calibration result,
not evidence that an external consumer processed the artifact once.

## Product pattern

1. Mint the logical artifact ID before the producing Activity.
2. Hash and durably publish content under an immutable content identity.
3. Publish a stable logical reference that binds ID, digest, size, and blob name; reject
   conflicting existing content.
4. Return only that compact reference through Temporal history.
5. Have the consumer verify bytes through the reference and publish a stable acknowledgement.
6. Reconcile pending references and unreachable blobs explicitly; never collect a blob
   reachable from a validated reference.
7. Treat SDK External Storage claims as transport references, not application receipts.

This pattern belongs in AI SDK artifact APIs and the effect-safe-tools cookbook. An SDK
helper can make the phases typed and observable, but must not collapse them into a single
"saved" boolean or imply exactly-once processing.

## Evidence and admission

The source-bound
[`large-artifact-20260812-v5`](../../experiments/large-artifact-durability/evidence/large-artifact-20260812-v5)
population contains 36 exact run directories, 397 files, and 36 replayed histories. Its
population-index SHA-256 is
`2bc24ebfb2bdf21e21db5ada9f7e1d30c192c25deca2b46f65f2184f06b28f56`.
The independent auditor reconstructed all run verdicts, exact inventories, hashes,
references, acknowledgements, runtime provenance, and replay. Seven coherent mutation
controls rejected source, history, verdict, identity, provenance, inventory, and symlink
tampering. The superseded v1 population remains preserved and rejects for incomplete SDK
provenance; v2 remains preserved and predates source-pin admission; v3 remains preserved
and predates the trusted runtime preregistration; v4 preregisters only the canonical
Worker and predates the separately pinned atomic-coverage Worker.

## Responsibility split

Temporal supplies durable Workflow procedure, Activity redelivery, persisted results,
compact external payload claims, and replay. The application supplies logical identities,
publication state, reference and acknowledgement protocols, and reconciliation. The
destination supplies durable conditional writes, conflict detection, retention, and any
atomicity between its own records.

## Limits and falsifier

The evidence is limited to Go SDK `1.47.0`, CLI `1.8.0`, Server `1.31.2`, one Linux host,
a local filesystem, one 393,216-byte artifact, sequential recovery, and one consumer. It
does not cover remote object stores, concurrent collectors, multipart uploads, cross-host
recovery, performance, retention expiry, or exactly-once consumer processing. Temporal
External Storage remains experimental and was evaluated only as payload offload.

The claim is falsified by protected byte loss or conflict, duplicate durable reference or
acknowledgement, reachable-blob collection, inline artifact bytes, unbound/tampered evidence,
failed retrieval, or history replay failure.
