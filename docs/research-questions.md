# Research questions

The lab asks whether the whole agent application remains correct after durable
execution recovers. Temporal product documentation is input to experiment design,
not proof of an application-level guarantee.

## Prioritized investigations

1. **Observed:** Worker death while an independently running agent survives:
   retry identity, reattachment, replacement, and stale-owner fencing.
2. A Worker dies after the durable launch decision but before child process
   registration: phantom ownership, reconciliation, and safe relaunch.
3. An external effect succeeds immediately before Activity completion is lost:
   idempotent and non-idempotent destinations, database/Git/message/artifact
   effects, reconciliation, and explicit ambiguity.
4. `CompleteActivityByID` after retry: compare attempt-scoped task-token
   completion, logical-ID completion, and application fencing on a live service.
5. Cancellation across Worker death, unreachable agents, ownership changes, and
   completion races.
6. Workflow and Activity evolution across deployments: replay compatibility,
   Worker Versioning, and agent-session compatibility contracts.
7. Partial streamed output: consumer-observed prefixes, retry duplication,
   ordering, reconstruction, and durable cursor placement.
8. Large artifacts: write/reference/persist/acknowledge failure windows and orphan
   reconciliation.
9. Workflows versus Standalone Activities and Nexus: when durable execution is
   enough and when durable orchestration adds a necessary state machine.
10. Operator diagnosis: healthy progress, retry loops, surviving or wedged agents,
   stale executors, ambiguous effects, and legitimate waits.

Milestone 1 changed the order rather than silently repairing the design. Review
exposed the launch-decision/registration gap as a concrete liveness hole, so its
failing reproduction now precedes new mechanism work. Source inspection also
identified `CompleteActivityByID` as an attempt-identity boundary requiring live
evidence; it is P1 alongside external-effect ambiguity.

## Cross-cutting questions

- Which identity is logical, which is a Temporal delivery identity, and which is
  only an OS/process address?
- Where is the linearization point for ownership, external effect, outcome, and
  acknowledgement?
- Can an old executor still exercise authority after replacement? Which
  destination rejects it?
- What compact state belongs in Event History, and what belongs behind a durable
  external reference?
- What observation would prove that a proposed guarantee is weaker than claimed?

The ordering changes when evidence exposes a higher-risk boundary. Changes and
their rationale are recorded in a decision or finding, not silently applied.
