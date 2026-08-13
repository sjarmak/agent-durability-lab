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
6. **Observed on one pinned Linux host with authenticated Claude Code `2.1.227`
   and Codex CLI `0.147.0`:** transcript/session/thread resume is not turn or
   effect authority; a fenced application supervisor passed the declared
   boundaries. [The matched study](plans/durable-vendor-agent-sessions.md) leaves
   Codex App Server, Claude Agent SDK/`SessionStore`, cross-host recovery, and
   version/session portability open.
7. Workflow and Activity evolution across deployments: replay compatibility,
   Worker Versioning, and agent-session compatibility contracts.
8. **Observed for one pinned Workflow Streams preview:** pre-flush process buffers
   disappeared, post-flush prefixes survived and were republished by a fresh Activity
   publisher, and explicit retry reset reconstructed one output. Closed-Workflow
   retention, reconnect, Continue-As-New composition, and external cursor durability
   remain open. See [Finding 0023](findings/0023-workflow-stream-retries-need-output-reconstruction.md).
9. **Observed on one pinned local-filesystem boundary:** large artifact blob,
   reference, Activity-completion, acknowledgement, and SDK External Storage windows.
   The protected application protocol converged in 18/18 runs; unsafe reference,
   acknowledgement, and SDK-offload controls duplicated in 9/9 distinguishing runs.
   Remote object stores, concurrent collection, multipart upload, retention, and
   performance remain open. See [Finding 0024](findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md).
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

The active [v2 authority and recovery-dynamics plan](plans/agent-durability-benchmark-v2.md)
is versioned beside v1 rather than changing it. Its required arms are Temporal
and PostgreSQL after their v1 conformance. It adds owner-label ABA reacquisition,
layered retry amplification, outage/backlog/herd recovery, backpressure, poison
isolation, and silent-progress detection. Restate, DBOS, and orchestration
topology are explicit later adoption work, so the expanded question does not
silently enlarge the frozen first-wave result.

The ordering changes when evidence exposes a higher-risk boundary. Changes and
their rationale are recorded in a decision or finding, not silently applied.
