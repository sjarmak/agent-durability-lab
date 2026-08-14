# Cancellation and cleanup

Canceling a Workflow is a durable request, not a revocation. A detached agent
keeps its authority until the application takes it away.

Back to the [guarantee summary](../guarantees.md).

## Workflow and Activity cancellation procedure

- **Temporal:** Workflow cancel and Activity cancel request are durable.
  `WaitForCancellation=true` waits for the Activity terminal event, which was
  Activity-canceled in the pinned histories but can be a heartbeat timeout after
  Worker death.
- **Your application:** the Activity must heartbeat or poll cancellation, and
  Workflow-context cancellation must still select disconnected cleanup when a
  dead Activity times out.
- **Your destination:** Temporal Service and an available compatible Worker.
- **Evidence:** [both wait policies observed across 24 historical trials](../findings/0006-cancellation-requires-application-revocation.md).
  Maintained live gates also admit the exact worker-death timeout race without
  weakening application revocation. The
  [native agent integration](../findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md)
  ran cleanup after cancellation while its termination control could not.

## Logical agent cancellation

- **Temporal:** no automatic revocation of a detached process.
- **Your application:** an atomic terminal cancellation revokes the active
  generation, blocks replacement, and rejects later registration, progress,
  effect, and outcome.
- **Your destination:** application-store durability and enforcement at every
  destination.
- **Evidence:** [all six Temporal-only controls mutated after Workflow cancellation; all 18 application-revoked runs accepted no mutation](../findings/0006-cancellation-requires-application-revocation.md).

## Cancellation reaches the exact executor tree

- **Temporal:** no.
- **Your application:** persist the exact session, generation, owner, PID, start,
  and group target, and signal and acknowledge separately.
- **Your destination:** Linux pidfd and process groups, process reachability and
  cooperation. Stronger containment may require cgroups.
- **Evidence:** [healthy, Worker-death, and frozen trees stopped under both wait policies](../findings/0006-cancellation-requires-application-revocation.md);
  the leader-only negative control left its child alive.

## Stale process cleanup

- **Temporal:** no.
- **Your application:** target the exact session, generation, and owner hash plus
  PID, start, and group identity, and separate revocation, delivery,
  acknowledgement, and disposition.
- **Your destination:** OS and process reachability and executor cooperation.
  Process-group signaling retains a validation and signal race.
- **Evidence:** [a delayed generation-1 stop leaves generation 2 alive; the leader-only negative control leaves its descendant alive](../findings/0006-cancellation-requires-application-revocation.md).
  Hostile, uncooperative, and cross-host cleanup are unresolved.

## Revocation of stale external credentials

- **Temporal:** no.
- **Your application:** issue generation-scoped, revocable authority and stop
  forwarding stale requests.
- **Your destination:** every destination must validate or revoke that authority.
- **Evidence:** work-store capability rejection was observed. Arbitrary copied
  API, Git, and message credentials are explicitly untested and not claimed.
