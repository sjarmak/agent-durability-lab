# Activity completion identity

This experiment tests whether asynchronous Activity completion identifies a
specific Activity attempt or only the logical pending Activity.

## Hypothesis

After attempt 1 times out and attempt 2 starts:

- completion with attempt 1's task token is rejected because the token is
  attempt-scoped;
- `CompleteActivityByID` issued on behalf of attempt 1 completes the currently
  pending logical Activity because the request does not carry attempt identity;
- an application fence can reject attempt 1 before it invokes the logical-ID
  completion API.

The source paths that motivated this hypothesis are not evidence for the
application-level result. The live service runs are the evidence.

## Invariant

Once attempt 2 owns the logical operation, attempt 1 must not be able to select
the accepted Workflow result.

## Exact boundary

The Activity returns `activity.ErrResultPending` for attempt 1. The Temporal
Service records its Start-to-Close timeout and starts attempt 2, which also
returns pending. Only after the harness has received both attempt observations
does it submit the stale completion.

No timing sleep is used to decide when attempt 2 exists. Attempt observations
are delivered through a bounded channel. Temporal compacts retry history, so
the final Event History is checked for attempt 2's start with attempt 1's
Start-to-Close timeout in `last_failure`, not for a retained attempt 1 start.

## Arms

| Arm | Stale action | Expected observation |
| --- | --- | --- |
| `stale-task-token` | Complete using attempt 1's task token | Service rejects it; attempt 2 completes and supplies the result |
| `stale-by-id` | Complete by Workflow ID, Run ID, and Activity ID on behalf of attempt 1 | Request succeeds and the stale result is accepted; unsafe control violates the invariant |
| `fenced-by-id` | Ask the application fence to authorize attempt 1 before completion by ID | Fence rejects attempt 1 without an RPC; attempt 2 completes by ID |

## Success, failure, and falsifier

A run is valid only if history contains attempt 2's start with attempt 1's
Start-to-Close timeout as its last failure, exactly one Activity completion,
and a Workflow result that matches the recorded completion.

The hypothesis is falsified if the stale task token completes attempt 2, if a
stale by-ID request is rejected as attempt-scoped, or if the application fence
allows an older attempt to invoke completion after attempt 2 is registered.

The conclusion is scoped to the exact Temporal Server, CLI, API, and Go SDK
versions in each evidence manifest.

## Run

```bash
make build
./bin/activity-completion-identity-experiment \
  --arm all \
  --trials 3 \
  --run-id local-completion-identity
```

Every trial gets a new append-only directory under `evidence/`. The runner
refuses to overwrite an existing run ID.

## Preserved result

The final `completion-identity-20260806-v2-*` directories contain three
qualifying trials for each arm. All nine verdicts are valid and match the
hypothesis. The `stale-by-id` control intentionally records
`invariant_satisfied: false`. Each accepted Temporal completion event is between
the matching `requested_at` and `responded_at` timestamps.

The `completion-identity-20260806-v1-*` set is preserved unchanged as pre-review
evidence. Its single `submitted_at` timestamp was captured after the operation
returned, so v1 supports the completion-identity result but is not used for
event-ordering claims.

`development-red-20260806-stale-task-token` preserves the first live harness
failure. Temporal returned `*serviceerror.NotFound`, but generic gRPC
`status.Code` classified the converted SDK error as `Unknown`; the original
oracle expected `NotFound`. This bundle is development evidence, not one of the
nine qualifying trials. The corrected harness uses `serviceerror.ToStatus` and
records both the normalized code and concrete error type.

Only task-token and owner-capability hashes are exported. Raw task tokens never
enter portable evidence; the ignored bbolt database is mode `0600`.

See [finding 0003](../../docs/findings/0003-activity-id-completion-is-not-attempt-scoped.md)
for the observed result, responsibility split, limitations, and falsifier.
