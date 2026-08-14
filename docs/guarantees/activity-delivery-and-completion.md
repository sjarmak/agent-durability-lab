# Activity delivery and completion

Temporal redelivers a lost Activity and accepts one completion. Which executor
is allowed to produce that completion is an application question.

Back to the [guarantee summary](../guarantees.md).

## Activity retry after Worker loss

- **Temporal:** the Server redelivers after timeout according to the retry policy.
- **Your application:** heartbeats and timeouts must make failure detectable, and
  the retry body must be safe to run again.
- **Your destination:** Temporal Service and an available Worker.
- **Evidence:** [observed after real Worker `SIGKILL`](../findings/0001-worker-death-surviving-agent.md);
  the compacted history records started attempt 2 with attempt 1's heartbeat
  timeout as `last_failure`.

## Stale asynchronous completion by task token

- **Temporal:** the pinned Server rejects attempt 1's task token after attempt 2
  starts.
- **Your application:** retain the attempt token and handle `NotFound` without
  treating it as application reconciliation.
- **Your destination:** Temporal Service version and token integrity.
- **Evidence:** [rejected in three live trials](../findings/0003-activity-id-completion-is-not-attempt-scoped.md);
  the Workflow accepted attempt 2's result.

## Stale asynchronous completion by logical Activity ID

- **Temporal:** no attempt-owner rejection was observed. The by-ID request
  completed the currently pending logical Activity.
- **Your application:** authorize an opaque current owner before invoking by-ID
  completion.
- **Your destination:** authority store durability and capability secrecy.
- **Evidence:** [the unsafe stale result was accepted in three trials, and the application-fenced arm rejected it](../findings/0003-activity-id-completion-is-not-attempt-scoped.md).

## Single accepted outcome

- **Temporal:** records one Activity completion that it receives.
- **Your application:** conditional terminal transition and terminal-state lookup
  on retry.
- **Your destination:** store durability.
- **Evidence:** all three arms expose one application outcome; the unsafe arm
  still produced duplicate effects.
