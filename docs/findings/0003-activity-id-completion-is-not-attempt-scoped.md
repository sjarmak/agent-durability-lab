# Finding 0003: Activity-ID completion is not attempt-scoped

Attempt 1's task token was rejected in three trials. Completion by Workflow,
Run, and Activity ID was accepted in three unsafe trials. The fenced arm
rejected it. By-ID completion is addressing, not ownership.

**Status:** observed in a final v2 set of three independent live trials per arm;
v1 remains preserved as pre-review evidence

**Versions:** Go 1.25.12; Temporal API 1.63.4; Temporal Go SDK 1.47.0;
Temporal CLI 1.8.0; Temporal Server 1.31.2; Linux amd64

## Claim

After asynchronous Activity attempt 1 times out and attempt 2 starts, completion
with attempt 1's task token is rejected. Completion by Workflow ID, Run ID, and
Activity ID is different: it resolves the currently pending logical Activity and
can accept a result submitted on behalf of obsolete attempt 1.

`CompleteActivityByID` is therefore a convenience addressing API, not an
application ownership fence. An application that permits detached executors to
complete by logical Activity ID must validate current ownership before making
the Temporal RPC.

## Observations

Every run captured attempts 1 and 2 through a channel before submitting a stale
completion. The final histories contain the compacted attempt-2 start with
attempt 1's Start-to-Close timeout in `last_failure`, one Activity completion,
and a Workflow result matching the one accepted completion.

In the three `stale-task-token` trials, the SDK returned
`*serviceerror.NotFound` for attempt 1's token. Attempt 2's token then completed
the Activity with `current-attempt-2`; the Workflow returned that value:
[trial 1](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-task-token-trial-1/observations.json),
[trial 2](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-task-token-trial-2/observations.json), and
[trial 3](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-task-token-trial-3/observations.json).

In the three unsafe `stale-by-id` trials, the request attributed to attempt 1
succeeded and selected `stale-attempt-1` as the Workflow result. A subsequent
completion with attempt 2's task token received `NotFound` because the logical
Activity was already complete:
[trial 1](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-by-id-trial-1/observations.json),
[trial 2](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-by-id-trial-2/observations.json), and
[trial 3](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-stale-by-id-trial-3/observations.json).

In the three `fenced-by-id` trials, attempt 2 atomically replaced attempt 1's
opaque owner capability in an application-owned bbolt store. Attempt 1's
capability was rejected as `stale_attempt` before a Temporal RPC. Attempt 2's
by-ID completion succeeded and the Workflow returned `current-attempt-2`:
[trial 1](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-fenced-by-id-trial-1/observations.json),
[trial 2](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-fenced-by-id-trial-2/observations.json), and
[trial 3](../../experiments/activity-completion-identity/evidence/completion-identity-20260806-v2-fenced-by-id-trial-3/observations.json).

V2 records `requested_at` before each RPC or fence lookup and `responded_at`
afterward. In all nine runs, Temporal's accepted Activity-completion event time
falls inside the corresponding request/response interval. V1 used a single
`submitted_at` value captured after the operation returned; review found that
label materially inaccurate. V1 was not rewritten and is excluded from the
timestamp claim.

The source-level reason that motivated, but did not prove, the experiment is
visible in the pinned Server implementation. A by-ID request resolves the
scheduled event from Activity ID, while the attempt comparison is conditional
on a non-empty scheduled-event ID:
[completion handler](https://github.com/temporalio/temporal/blob/v1.31.2/service/history/api/respondactivitytaskcompleted/api.go) and
[token validation](https://github.com/temporalio/temporal/blob/v1.31.2/service/history/api/activity_util.go).
The Go SDK constructs the two request forms separately:
[SDK implementation](https://github.com/temporalio/sdk-go/blob/v1.47.0/internal/internal_workflow_client.go).

## Responsibility split

- Temporal rejected the obsolete task token and durably recorded one Activity
  completion and Workflow result.
- Temporal accepted logical-ID completion for the pending Activity; that API did
  not carry the stale caller's attempt identity.
- The application supplied monotonic attempt registration, opaque owner
  capabilities, same-attempt competitor rejection, and pre-RPC authorization.
- The SDK exposed the stale token as `*serviceerror.NotFound`. Generic gRPC
  `status.Code` reported `Unknown` for that converted SDK error; the harness now
  uses `serviceerror.ToStatus` and preserves the original type and message.

Temporal's task-token protection applies when the caller retains and uses the
task token. It does not transfer to a less-specific logical identifier.

## Scope — what this does not show

- Behavior on Server, API, or SDK versions other than those recorded above.
- Semantics when `runID` is omitted or when the Activity has not started; the
  Server contains a separate path that can fabricate a started event.
- Completion-by-ID semantics for Standalone Activities.
- Safety if an obsolete process steals the current opaque application
  capability.
- Cancellation, failure, heartbeat, or asynchronous completion APIs other than
  successful Workflow Activity completion.
- Whether authorization and a downstream effect can be made atomic at a remote
  destination. That is the next external-effect experiment.

## What would change this conclusion

This finding is narrowed or falsified if a repeated pinned-version run accepts
attempt 1's task token after attempt 2 starts, rejects attempt 1's by-ID request
as attempt-scoped, records more than one Activity completion, returns a Workflow
result different from the accepted completion, or allows an obsolete owner
capability through the application fence.
