# Guarantee ledger

This table records mechanisms and current evidence. "No" does not mean Temporal
is defective; it means the property is outside the cited Temporal guarantee.

| Property | Provided by Temporal | Provided by application | Depends on external system | Evidence |
| --- | --- | --- | --- | --- |
| Workflow state durability | Event History and replay preserve recorded Workflow procedure | Deterministic Workflow code and compatible evolution | Temporal Service persistence/availability | [Captured history replays](../experiments/worker-death/README.md#preserved-milestone-run); incompatible timer change rejected with `TMPRL1100` |
| Activity retry after Worker loss | Server redelivers after timeout according to retry policy | Heartbeats/timeouts must make failure detectable; retry body must be safe | Temporal Service and an available Worker | [Observed after real Worker `SIGKILL`](findings/0001-worker-death-surviving-agent.md); compacted history records started attempt 2 with attempt 1's heartbeat timeout as `last_failure` |
| External exactly-once effect | No | Idempotency/deduplication, transaction, fencing, or reconciliation as appropriate | Destination must implement the required atomicity | Not claimed; unsafe Worker-death control accepted two synthetic effects, while the effect-success/completion-loss window remains untested |
| Stable logical agent identity | No automatic mapping to an external process | Stable session key and durable process/session registry | Store durability and process reachability | [One session across two attempts observed](../experiments/worker-death/evidence/milestone1-20260806-v3-reattach/application-state.json), single host only |
| No duplicate launch on retry | No | Atomic start-or-attach plus explicit `launch_pending`/`running` lifecycle | Store serialization and an executor that registers before effects | [Registered child reattaches](../experiments/worker-death/evidence/milestone1-20260806-v3-reattach/application-state.json); [pending launch is conditionally replaced](../experiments/worker-death/evidence/launch-gap-20260806-v3-fenced-recovery/application-state.json); unsafe control launches twice |
| Liveness after pre-launch Worker death | Activity retry only; Temporal does not know if `exec` happened | Pending-launch detection and fenced conditional replacement | Available Worker, process launcher, and work store | [Blind attach stalls on a phantom](findings/0002-launch-decision-is-not-process-liveness.md); fenced recovery completes in two preserved trials |
| Stale-writer rejection | No | Monotonic attempt check for replacement; generation/token and `running`-state checks at every authoritative mutation | Destination must enforce or delegate the fence | Fenced run events 15–18 show replacement accepted before stale effect/completion rejection; launch-gap tests reject an older replacement attempt and a pending executor's mutations |
| Single accepted outcome | Temporal records one Activity completion that it receives | Conditional terminal transition and terminal-state lookup on retry | Store durability | All three arms expose one application outcome; unsafe arm still produced duplicate effects |
| Cancellation reaches exact session | Cancellation is recorded/delivered to an Activity when polled/heartbeated | Map cancellation to current fenced session and reconcile unreachable processes | Process control plane/reachability | Unresolved |
| Artifact durability | History durably stores payloads within supported limits | Durable content-addressed object plus atomic reference/outbox | Artifact store durability | Unresolved |
| Stream reconstruction | No application-level claim yet | Durable sequence/cursor and deduplication are hypotheses | Stream transport and consumer acknowledgement | Unresolved |
| Duplicate message suppression | Task delivery/retry is durable, not destination deduplication | Stable message/effect key and acknowledgement protocol | Destination deduplication/transactions | Unresolved |
| Distinguish wedged application from healthy retry | Temporal exposes task/Workflow state, not external-process truth | Correlate retry attempt, launch lifecycle, PID/process identity, progress, and outcome age | Process registry and clock/telemetry integrity | Launch-gap control records attempt-2 application code attached to `launch_pending` with PID 0; server-visible heartbeat evidence and operator policy for wedged `running` work remain unresolved |

Each completed experiment replaces "pending" with a link to raw evidence, its
finding, exact versions, and the falsifier. Documentation alone never upgrades an
application-level property to observed.
