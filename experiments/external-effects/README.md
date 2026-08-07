# External-effect ambiguity

This experiment tests the boundary where an Activity mutates a destination,
the Worker is killed before it can report completion, and Temporal retries the
Activity on another Worker.

## Invariant

For one logical effect ID, recovery leaves one physical effect and the Workflow
returns the receipt for that effect.

The unsafe control is expected to falsify the invariant. The protected arm is
expected to satisfy it under the stated assumptions. A passing protected arm is
not evidence that Temporal made the external effect exactly once.

## Exact failure boundary

Attempt 1 performs and durably observes the destination mutation, then blocks at
the `after-effect/attempt-1` barrier before returning from the Activity. The
coordinator observes that exact arrival and sends `SIGKILL` to Worker 1. Worker 2
then receives the Start-to-Close retry and runs attempt 2.

There are no timing sleeps in boundary selection. Timestamps must establish:

```text
effect response <= barrier arrival <= Worker SIGKILL <= attempt 2 start
```

## Destination arms

| Destination | Unsafe control | Smallest protected mechanism | Protection dependency |
| --- | --- | --- | --- |
| Idempotent HTTP API | POST without a key | stable idempotency key | destination atomically stores key with response |
| Non-idempotent HTTP API | repeat POST | query by stable correlation ID before retry | strongly consistent lookup and no concurrent same-ID caller |
| Transactional database | attempt-specific insert | unique logical effect key in the mutation transaction | database atomicity and uniqueness |
| Git worktree | attempt-specific commit | stable marker path plus content/commit reconciliation | serialized access to this worktree |
| Message publication | publish without message ID | destination-side message-ID deduplication | tested only against the hermetic destination; real broker semantics remain untested |
| Artifact creation | attempt-specific files | content-addressed blob plus stable reference | atomic file publication and content validation |

The non-idempotent API remains non-idempotent: it appends on every POST. Its
protected arm avoids the second POST by reconciling a correlation ID that the
destination records and exposes. This does not protect check-then-act races or a
destination that cannot be queried.

The artifact arm injects failure after both blob and reference are durable. It
does not by itself prove safety in the narrower blob-written/reference-missing
window; that is a separate failure boundary.

## Success, failure, and falsifier

A run is valid only if two Activity Task Executions are observed, attempt 1 is
timed out after Worker 1 is killed at the barrier, exactly one Activity
completion is recorded, and the Workflow returns attempt 2's receipt.

The protected conclusion is falsified by two physical effects, different
receipts across attempts, retry starting before the kill, a non-timeout retry,
or a completed Workflow whose receipt does not identify the surviving effect.
The unsafe control is expected to leave two physical effects.

Every final evidence set uses three fresh trials per destination and arm. Each
run directory is append-only and contains its manifest, observations,
destination snapshot, verdict, Temporal history, Temporal server log, and both
Worker logs.

## Run

```bash
make build
./bin/external-effect-experiment \
  --destination all --mode all --trials 3 --run-id local-effects
```

The CLI also accepts one destination and one mode. Destination names are
`idempotent-api`, `non-idempotent-api`, `database`, `git`, `message`, and
`artifact`.

## Preserved evidence

The final evidence is a 36-run set on Go 1.25.12, Temporal API 1.63.4, Go SDK
1.47.0, CLI 1.8.0, and Server 1.31.2:

- `external-effects-20260806-v1-*` contains the 18 final HTTP/API and database
  trials.
- `external-effects-20260806-v2-*` contains the 18 final Git, message, and
  artifact trials.

V1 then stopped on its first Git run because the evidence exporter passed a
caller-relative bundle path to `git -C`. That invalid run remains at
`external-effects-20260806-v1-git-unsafe-trial-1` with partial observations,
partial Temporal history, Worker logs, and the failure. V2 uses an absolute
export target and preserves six verified Git bundles.

An earlier race-enabled matrix exposed a 1.5-second Start-to-Close timeout that
could expire before the Activity reached its first application observation.
That invalid run remains under
`development-red-20260806-premature-timeout-database-protected`; the regression
test fixes the detection window at five seconds. Neither invalid run contributes
to the semantic conclusion.
