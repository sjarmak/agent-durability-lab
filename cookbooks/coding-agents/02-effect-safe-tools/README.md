# Effect-safe tools after Activity retry

Use these recipes when a coding-agent Activity can mutate something outside
Temporal and then lose its completion. They are executable views of
[Finding 0004](../../../docs/findings/0004-one-temporal-completion-can-hide-two-effects.md)
and the [external-effects experiment](../../../experiments/external-effects/README.md),
not a second implementation of the experiment.

## Question, invariant, boundary, and oracle

**Question:** After an Activity's destination mutation succeeds but its Worker
dies before returning, what is the smallest credible protection for each common
tool destination?

**Invariant:** One stable logical effect ID leaves one physical destination
effect, and the Workflow returns that effect's original receipt after retry.

**Exact failure boundary:** Attempt 1 receives the destination response and
durably records its receipt, reaches `after-effect/attempt-1`, and blocks before
the Activity returns. The controller observes that exact barrier and sends
`SIGKILL` to attempt 1's recorded Worker/PID. Attempt 2 starts only after that
kill and the Start-to-Close timeout. No sleep chooses the boundary.

```text
attempt 1 effect response <= barrier arrival <= Worker 1 SIGKILL <= attempt 2 start
```

**Success/failure oracle:** A valid run has attempts 1 and 2, a timed-out first
attempt, exactly one `ActivityTaskCompleted` event for attempt 2, and a Workflow
result equal to attempt 2's receipt. Separately, `destination-state.json` counts
physical effects. The unsafe control must leave two effects and distinct
receipts. The protected arm must leave one effect and return the same receipt
from both attempts. Temporal history is not used as the physical-effect oracle.

**Falsifier:** Reject the conclusion if an unsafe valid run has only one effect;
if a protected run has two effects, mismatched receipts, or accepts conflicting
content; if attempt 2 starts before the exact kill; or if the recorded
assumptions do not hold.

## The six recipes

The effect ID belongs to the application and remains unchanged across Activity
attempts. An Activity attempt number, task token, Worker PID, and Workflow Run ID
are not substitutes for that stable identity.

| Destination | Unsafe / protected operation | Required contract | Conflict rule | Preserved receipt/artifact | Limits |
| --- | --- | --- | --- | --- | --- |
| Idempotent API | POST without a key / repeat POST with the effect ID as idempotency key | Destination atomically stores key, mutation, and response; it serializes a key and retains it through redelivery | Same key with another effect ID or payload is rejected | Both attempt receipts and exported API state | Retention expiry or non-atomic key handling reopens duplication |
| Reconciled non-idempotent API | Repeat POST / strongly consistent correlation lookup before retry POST | Append stores correlation, content identity, and receipt; one same-ID caller at a time | Lookup rejects a different payload hash | Both attempt receipts and exported append state | This is sequential reconciliation, not protection from a check-then-POST race |
| Transactional DB | Attempt-specific key / effect ID as unique key in the mutation transaction | Uniqueness, mutation, and receipt commit in one transaction; row outlives redelivery | Existing key with different content is rejected | Both attempt receipts and portable state export | No atomicity with another remote system; the runtime DB is not portable evidence |
| Git | Attempt-specific files/commits / stable marker content plus commit lookup | Serialized worktree; marker and reachable commit retained | Existing stable marker with different content is rejected | Both receipts, state export, and verified repository bundle | Concurrent writers and failure after file write but before commit are outside this boundary |
| Message publication | Publish without an ID / publish with the effect ID as message ID | Broker atomically retains ID, publication, and receipt longer than redelivery | Same message ID with different content is rejected | Both receipts and exported publication state | The hermetic result does not establish broker retention, consumer acknowledgement, or processing semantics |
| Artifact creation | Attempt-specific blob/reference/ack / content-addressed blob plus stable logical reference and consumer acknowledgement | Durably publish blob, pending reference, immutable reference, then stable acknowledgement; explicitly reconcile pending and unreachable content | Existing blob, reference, or acknowledgement with different content is rejected | Source hash, actual blob/reference/ack, pre-fault/final inventories, reconciliation receipt, and replayable history | [Local sequential boundaries are observed](../../../docs/findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md); remote stores, concurrent GC, multipart upload, and retention remain untested |

The non-idempotent API remains non-idempotent. Its protection is a lookup that
avoids the second POST after attempt 1 already appended. Git uses the same
sequential reconciliation shape. The other four rely on destination-side
atomic deduplication or uniqueness. There is deliberately no universal wrapper
and no generic exactly-once claim.

## Audit the cited final evidence

From a fresh checkout at repository root:

```bash
go test -race -cover ./cookbooks/coding-agents/02-effect-safe-tools
go run ./cookbooks/coding-agents/02-effect-safe-tools audit
go test -race ./experiments/external-effects/internal/lab \
  -run 'TestProtectedDestinationsRejectConflictingPayloadWithoutMutation|TestProtectedGitRejectsConflictingMarker'
```

The audit is read-only. It checks all 36 cited final runs (three trials for each
unsafe/protected arm), exact boundary ordering and process identity, stable
effect identity, Temporal completion cardinality, independent physical-effect
counts, receipts, verdicts, required raw files, all six Git bundles, and every
preserved artifact blob/reference. It never regenerates or rewrites evidence.

Final evidence prefixes are `external-effects-20260806-v1-*` for both API arms
and the database, and `external-effects-20260806-v2-*` for Git, message, and
artifact arms. The invalid and superseded runs documented by Finding 0004 stay
preserved but do not enter the 36-run result.

**Observed result:** all 18 unsafe trials produced two physical effects with
different receipts. All 18 protected trials produced one physical effect and
returned attempt 1's receipt from attempt 2. Every valid Temporal history
contained one Activity completion and every Workflow returned attempt 2's
receipt.

## Run one fresh live recipe

Install the pinned Temporal CLI named by the experiment README, then choose a
new output directory outside the preserved evidence tree:

```bash
fresh_root="$(mktemp -d)"
go run ./cookbooks/coding-agents/02-effect-safe-tools run \
  --destination idempotent-api \
  --output "$fresh_root/evidence" \
  --run-id cookbook-idempotent-api \
  --trials 3
```

Replace `idempotent-api` with exactly one of the other five names printed by:

```bash
go run ./cookbooks/coding-agents/02-effect-safe-tools list
```

The runner builds the experiment Worker in a temporary directory, then executes
both the unsafe and protected arms through the original live harness. It refuses
an output path inside `experiments/external-effects/evidence`, so the cited raw
evidence remains append-only. The generated run directory contains the
manifest, observations, independent destination snapshot, verdict, Temporal
history/server log, Worker logs, and destination-specific portable artifacts.

## Responsibility and limits

Temporal supplies durable retry and one accepted Activity completion. The
application supplies the stable effect ID, chooses one of these protocols,
validates content, and reconciles where needed. The destination supplies the
specific atomicity, lookup, serialization, and retention property. The lab
controller supplies the exact fault barrier and independent oracle.

The evidence uses the versions recorded in Finding 0004 and kills/reaps Worker
1 before attempt 2 begins. It does not cover overlapping attempts, other SDK or
server versions, arbitrary real APIs/brokers/object stores, or atomicity across
two destinations. Activity retries alone do not make an external effect execute
exactly once.
