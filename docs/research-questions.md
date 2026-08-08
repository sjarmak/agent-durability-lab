# Research questions

The lab asks whether the whole agent application remains correct after durable
execution recovers. Temporal product documentation is input to experiment design,
not proof of an application-level guarantee.

## Prioritized investigations

1. **Observed:** Worker death while an independently running agent survives:
   retry identity, reattachment, replacement, and stale-owner fencing.
2. **Observed on both sides of `exec`:** a Worker dies after the durable launch
   decision but before child registration. The same PID-less `launch_pending`
   state represented either a phantom or a live unregistered child; trusted
   discovery enabled attach, while generation replacement rejected stale
   registration and observed cooperative cleanup.
3. **Observed at the post-effect/pre-completion boundary:** idempotent and
   non-idempotent APIs, database, Git, message, and artifact effects. All unsafe
   retries duplicated the effect; six destination-specific mechanisms prevented
   duplication under their recorded assumptions.
4. **Observed:** `CompleteActivityByID` after retry: attempt-scoped task-token
   completion rejects the stale token, logical-ID completion accepts the stale
   result, and an application capability fence rejects the obsolete owner.
5. **Observed on one Linux host:** Temporal-only cancellation leaves detached
   authority intact; application revocation plus exact process-tree delivery
   survives Worker death and a frozen executor under both wait policies.
   Completion/cancellation ordering and delayed stale-stop isolation are covered
   by deterministic store/process tests. Cross-host and hostile containment
   remain open.
6. [Durable Claude Code and Codex sessions](plans/durable-vendor-agent-sessions.md):
   distinguish transcript resume from turn/process recovery and workspace/effect
   correctness; compare unsafe CLI retry, vendor resume, and a fenced
   start-or-attach supervisor.
7. Workflow and Activity evolution across deployments: replay compatibility,
   Worker Versioning, and agent-session compatibility contracts.
8. Partial streamed output: consumer-observed prefixes, retry duplication,
   ordering, reconstruction, and durable cursor placement.
9. Large artifacts: write/reference/persist/acknowledge failure windows and orphan
   reconciliation.
10. Workflows versus Standalone Activities and Nexus: when durable execution is
   enough and when durable orchestration adds a necessary state machine.
11. Operator diagnosis: healthy progress, retry loops, surviving or wedged agents,
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

## Cross-system comparison

The first comparison wave is Temporal, Restate, DBOS Go, and a minimal
PostgreSQL queue/lease/outbox. The [v1 benchmark contract](../benchmarks/agent-durability/README.md)
holds the external agent, authority protocol, effect destination, failure
schedule, evidence, and oracle fixed while allowing idiomatic durable-procedure
adapters. Native-minimum, portable-safety, and optional native-optimized arms
are reported separately so a co-transactional product feature is not mistaken
for an intrinsic advantage on an unmatched workload.

The contract, append-only writer, independent oracle, and live common apparatus
are implemented and calibrated, but this is still not comparison evidence. No
durability-system adapter is conformant yet. Durable Task and AWS Step Functions
remain deferred until the first wave identifies a decision that their
architecture could change.

The ordering changes when evidence exposes a higher-risk boundary. Changes and
their rationale are recorded in a decision or finding, not silently applied.
