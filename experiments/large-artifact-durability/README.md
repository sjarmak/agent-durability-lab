# Large artifact durability

## Status

Observed on one Linux `amd64` host with Go `1.25.12`, Temporal Go SDK `1.47.0`,
Temporal CLI `1.8.0`, and Server `1.31.2`. The application protocol is the product
pattern supported by this experiment. Temporal External Storage remains experimental
payload offload and does not provide application acknowledgement or exactly-once effects.

## Question and invariant

What must a coding-agent application do when a large output, patch, or transcript crosses
separate blob-write, reference-publication, Activity-completion, and consumer-acknowledgement
boundaries?

For one logical artifact ID and immutable digest, the protected protocol must converge
after redelivery to one verified content-addressed blob, one immutable logical reference,
one compact reference in Event History, and one durable consumer acknowledgement. The
393,216-byte artifact must never enter Workflow history.

## Failure boundaries

Worker 1 receives `SIGKILL` after an authenticated, single-use barrier observes one exact
point:

- `blob_published`: the blob is durable, but no reference exists;
- `reference_created`: a pending reference is durable, but not yet published;
- `reference_published`: the durable reference exists, but the producer Activity has not completed;
- `activity_completed`: Temporal has recorded producer completion, but no acknowledgement exists;
- `acknowledgement_published`: the acknowledgement is durable, but its Activity has not completed; or
- `external_storage_stored`: the SDK StorageDriver object is durable, but its claim has not returned.

Worker 2 receives the redelivered work. Unsafe controls use attempt-specific names;
protected application runs use a content digest plus stable logical reference and
acknowledgement identities.

## Oracle

Each run records the source digest/size, exact pre-fault and final inventories, pending
and removed orphans, logical reference and acknowledgement, Worker/PID/attempt identity,
authenticated barrier receipt, raw Event History, runtime/executable provenance, and
replay result. The independent disk auditor reads a hash-sealed exact inventory once,
reconstructs the durable store and verdict, retrieves the artifact, and replays history.
The population auditor requires the exact 36-run schedule and manifest hashes.

## Run

Run unit, integration, real-service, child-process, and coverage gates:

```bash
make coverage-large-artifact
```

Audit the admitted population without credentials or a running Temporal Service:

```bash
go run ./experiments/large-artifact-durability/cmd/evidence-audit \
  --population "$PWD/experiments/large-artifact-durability/evidence/large-artifact-20260812-v5"
```

Fresh evidence requires an absent population directory and the Temporal CLI on `PATH`:

```bash
mkdir experiments/large-artifact-durability/evidence/large-artifact-YYYYMMDD-vN
LARGE_ARTIFACT_LIVE=1 \
LARGE_ARTIFACT_EVIDENCE_ROOT="$PWD/experiments/large-artifact-durability/evidence/large-artifact-YYYYMMDD-vN" \
go test -race -count=1 -timeout 12m -v \
  ./experiments/large-artifact-durability/internal/lab \
  -run '^TestLiveLargeArtifactDurabilityMatrix$'
```

## Evidence and observed result

The admitted [`large-artifact-20260812-v5`](evidence/large-artifact-20260812-v5)
population index has SHA-256
`2bc24ebfb2bdf21e21db5ada9f7e1d30c192c25deca2b46f65f2184f06b28f56`.
All 36 runs were valid, matched their registered observation, and replayed. All 18
protected runs satisfied the invariant.

- Unsafe blob and pending-reference failures created an orphan that explicit
  reconciliation removed; protected retry reused the existing blob/reference state.
- All three unsafe reference-publication trials retained two durable references;
  all protected trials retained one.
- Activity-completion was a calibration boundary: both modes converged because Temporal
  had already persisted the producer result before the consumer began.
- All three unsafe acknowledgement-publication trials retained two acknowledgements;
  protected trials retained one.
- All three unsafe External Storage trials retained two payload objects; protected
  content-addressed storage retained one. This comparison says nothing about consumer ack.

The complete v1 population remains preserved but is rejected by the current auditor
because its SDK version provenance was `unknown`. The complete v2 population also remains
preserved but is rejected because it predates source-pin admission. Neither population was
rewritten or admitted. V3 is likewise preserved but rejected because it predates the
source-pinned runtime preregistration that anchors executable identity.
V4 is preserved but rejected because it preregistered only the canonical Worker, not the
separately pinned atomic-coverage Worker used by the coverage gate.

## Responsibility split

- Temporal durably records procedure, retries, Activity results, compact StorageDriver
  claims, and replayable history.
- The application owns logical artifact/reference/consumer identities, content validation,
  publication order, acknowledgement, and explicit orphan reconciliation.
- The storage destination owns durable blob/reference/ack writes, conflict rejection,
  retention, and atomicity within each operation.

## Limits and falsifier

This is a single-host filesystem experiment with fixed 393,216-byte content, two Workers,
one consumer, and sequential reconciliation. It does not establish remote object-store
consistency, concurrent GC safety, cross-host recovery, retention policy, multipart upload,
arbitrary payload sizes, performance, or exactly-once delivery. External Storage is an SDK
comparison, not the protected application protocol.

The result is falsified if protected recovery loses or changes bytes, accepts a conflicting
digest, retains multiple durable references or acknowledgements, deletes a reachable blob,
places artifact bytes inline in Event History, cannot retrieve through the stored reference,
admits a tampered inventory, or fails replay.
