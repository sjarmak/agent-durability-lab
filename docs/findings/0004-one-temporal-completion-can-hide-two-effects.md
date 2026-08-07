# Finding 0004: One Temporal completion can hide two external effects

**Status:** observed in 36 valid live trials: three trials for each unsafe and
protected arm across six destination classes; two invalid harness runs remain
preserved and are excluded

**Versions:** Go 1.25.12; Temporal API 1.63.4; Temporal Go SDK 1.47.0;
Temporal CLI 1.8.0; Temporal Server 1.31.2; Linux amd64

## Claim

Temporal can durably recover an Activity and record exactly one Activity
completion while the application has performed the external effect twice.
Activity completion cardinality is not external-effect cardinality.

Preventing the duplicate required a stable logical effect ID plus a mechanism
at, or supported by, each destination. No single generic mechanism covered all
six destinations, and this experiment does not claim external exactly-once
execution.

## Failure boundary and oracle

Attempt 1 mutated the destination and durably recorded the returned receipt. It
then arrived at `after-effect/attempt-1` and blocked before returning from the
Activity. The controller checked that one physical effect and a finished attempt
observation existed, verified the arrival's effect ID, Worker ID, and PID, then
sent `SIGKILL` to that exact Worker process. Worker 2 received attempt 2 after
the Start-to-Close timeout.

Every valid run established this order with recorded timestamps:

```text
attempt 1 effect response <= barrier arrival <= Worker 1 SIGKILL <= attempt 2 start
```

The Temporal history had attempt 1's Start-to-Close timeout in the compacted
retry failure, attempt 2 as the completed attempt, and one
`ActivityTaskCompleted` event. The application oracle independently counted the
destination's physical effects and compared both attempt receipts.

## Observations

All 18 unsafe trials left two physical effects with different receipts. All 18
protected trials left one physical effect and returned the same receipt to both
attempts. Every Workflow returned attempt 2's receipt.

| Destination | Protected attempt-2 result | Dependency demonstrated | Example control / protected evidence |
| --- | --- | --- | --- |
| Idempotent HTTP API | `deduplicated` | destination atomically retains idempotency key with effect/receipt | [control](../../experiments/external-effects/evidence/external-effects-20260806-v1-idempotent-api-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v1-idempotent-api-protected-trial-1/observations.json) |
| Non-idempotent HTTP API | `reconciled` | destination exposes strongly consistent lookup by correlation ID; retry queries before POST | [control](../../experiments/external-effects/evidence/external-effects-20260806-v1-non-idempotent-api-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v1-non-idempotent-api-protected-trial-1/observations.json) |
| Transactional database | `deduplicated` | logical effect ID is the unique key in the same bbolt transaction as the mutation | [control](../../experiments/external-effects/evidence/external-effects-20260806-v1-database-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v1-database-protected-trial-1/observations.json) |
| Git worktree | `reconciled` | stable marker path/content and existing commit receipt are checked before mutation | [control](../../experiments/external-effects/evidence/external-effects-20260806-v2-git-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v2-git-protected-trial-1/observations.json) |
| Message publication | `deduplicated` | destination atomically retains a stable message ID with the publication receipt | [control](../../experiments/external-effects/evidence/external-effects-20260806-v2-message-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v2-message-protected-trial-1/observations.json) |
| Artifact creation | `deduplicated` | content-addressed blob plus stable effect reference, atomically published and content-validated | [control](../../experiments/external-effects/evidence/external-effects-20260806-v2-artifact-unsafe-trial-1/observations.json) / [protected](../../experiments/external-effects/evidence/external-effects-20260806-v2-artifact-protected-trial-1/observations.json) |

The six Git bundle files pass `git bundle verify`. Artifact evidence preserves
the actual blobs and references. API, database, and message destinations export
their physical state to `destination-state.json`; runtime databases are not
treated as portable evidence.

Temporal's documentation describes this crash window and recommends idempotent
Activities. It also states that an Activity may execute or partially complete
more than once even though Temporal observes it as completed once:
[Activity idempotency and retry](https://docs.temporal.io/activity-definition#idempotency).
That documentation motivated the boundary; the live runs above are the evidence
for this application and these destination mechanisms.

## Responsibility split

- Temporal detected the missing completion through Start-to-Close timeout,
  durably scheduled attempt 2, and recorded one completed Activity and Workflow
  result.
- The application carried a stable effect ID across attempts, chose the
  destination-specific protocol, rejected conflicting idempotency content, and
  reconciled destinations that could not reject duplicates themselves.
- The external destination supplied the atomic key/effect transaction for the
  idempotent API, database, and message arms. Without that destination support,
  an application pre-check is not safe against concurrent same-ID callers.
- The experiment controller supplied exact fault timing and an independent
  physical-effect oracle. These are evidence mechanisms, not production
  guarantees.

The non-idempotent API was not made idempotent. It still appends on every POST;
the protected retry found attempt 1's correlation receipt and did not issue the
second POST. Likewise, Git reconciliation assumes serialized access to this
worktree. These are sequential-retry results, not concurrent exactly-once
claims.

## Preserved invalid runs

The race-enabled development matrix first used a 1.5-second Start-to-Close
timeout. One database run timed out before attempt 1 reached the application
observation, so the controller correctly refused to claim the requested
boundary. Its log and failure remain at
[`development-red-20260806-premature-timeout-database-protected`](../../experiments/external-effects/evidence/development-red-20260806-premature-timeout-database-protected/).
A regression fixes the detection window at five seconds.

The final v1 run completed all 18 API/database trials, then failed while exporting
the first Git bundle because a relative output path was interpreted from inside
`git -C`. Partial observations and history remain in
[`external-effects-20260806-v1-git-unsafe-trial-1`](../../experiments/external-effects/evidence/external-effects-20260806-v1-git-unsafe-trial-1/).
V2 resolves the caller path before invoking Git and supplies the 18 final
Git/message/artifact trials. Neither harness failure changes an external-effect
claim, and neither is counted among the 36 valid runs.

## What this does not establish

- Behavior on Temporal or Go SDK versions other than those recorded above.
- Safety when attempt 1 and attempt 2 execute concurrently; this run kills and
  reaps Worker 1 before Worker 2 starts.
- Safety for a non-idempotent API that cannot expose a stable correlation
  lookup, or when two callers can race between lookup and POST.
- Git safety with concurrent worktree writers or failure after file write but
  before commit.
- Broker behavior after deduplication retention expires, consumer
  acknowledgement semantics, or exactly-once processing.
- Artifact safety between blob publication and reference publication, orphan
  collection, or durability on a remote object store.
- Atomicity between application reconciliation state and a separate remote
  destination.

## Falsifier

The broad finding is falsified if a valid unsafe run records one physical effect
despite two applied attempts, or if Temporal records two Activity completions for
this history. A protected destination claim is narrowed or falsified if a fresh
pinned-version run leaves two physical effects, returns different receipts,
accepts conflicting content for one key, starts attempt 2 before the exact Worker
1 kill, or depends on an assumption not listed above.
