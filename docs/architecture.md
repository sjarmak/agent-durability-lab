# Laboratory architecture

The initial architecture is a hypothesis to test, not a universal prescription.

## Generic shape

Every implementation in this lab fits the same three-role boundary:

```text
Durable coordinator
       │
       │ delivery / retry / wait
       ▼
Agent executor ──────────────► external system
       │
       ▼
authority / evidence store
```

- **Durable coordinator** — records and recovers procedure: ordering, retries,
  waits, cancellation requests, and accepted completion.
- **Agent executor** — the process (or simulator) that performs the work and
  attempts the external effect.
- **Authority / evidence store** — owns logical identity, current ownership
  generation, and the append-only record a later attempt must consult.

Implementations tested so far:

| Role | Implementations tested |
| --- | --- |
| Durable coordinator | Temporal, PostgreSQL queue/lease/outbox |
| Agent executor | Claude Code, Codex, simulator |
| External system | Git, API, database, broker, artifact store |

Restate and DBOS are follow-up coordinator adapters, not yet run.

Temporal is currently the lab's most extensively instrumented durable-execution
implementation: native history replay, Activity retry boundaries, cancellation
behavior, Worker deployment versioning, and the topology experiments all live
there. Product-specific observations are labeled as such throughout this repo;
a cross-system claim requires evidence from the portable benchmark, not from
Temporal alone.

## Coordinator adapter contract

The [cross-system benchmark](../benchmarks/agent-durability/README.md) holds
workload, failure schedule, evidence, and oracle fixed while letting each
coordinator implement the following contract idiomatically:

```go
type DurabilityAdapter interface {
    Start(ctx context.Context, op Operation) (Execution, error)
    AwaitDelivery(ctx context.Context, id LogicalWorkID) (Delivery, error)
    RecordCompletion(ctx context.Context, d Delivery, result Result) error
    Cancel(ctx context.Context, id LogicalWorkID) error
    Inspect(ctx context.Context, id LogicalWorkID) (ExecutionState, error)
}
```

The benchmark contract cares about stable logical identity, redelivery,
authority, cancellation, externally visible effects, and recovery cost/
liveness — not about forcing every coordinator into a Workflow/Activity shape.
Temporal and the PostgreSQL queue/lease/outbox adapter both satisfy this
contract today; see [`benchmarks/agent-durability/`](../benchmarks/agent-durability/)
for the concrete adapters and [`contract-v1.json`](../benchmarks/agent-durability/contract-v1.json)
for the machine-checked case list.

## Temporal implementation

The rest of this document, and most of the evidence in the lab, describes the
Temporal binding of the generic shape above:

```text
Experiment controller ── starts/kills ──> Temporal Worker
        │                                      │
        │ exact barriers                       │ Activity attempt
        ▼                                      ▼
 Failure-injection service              agent process launcher
                                               │ detached child
                                               ▼
Temporal Service <── history/task state   agent simulator ──> external effect
                                               │
                                               ▼
                                  application work/evidence store
```

Under this binding, the durable coordinator role is Temporal (Worker plus
Service), the agent executor role is the agent process launcher and simulator,
and the authority/evidence store role is the application work/evidence store.
The PostgreSQL adapter binds the same roles to a queue table claimed with
`FOR UPDATE SKIP LOCKED`, a lease/generation column, and the same application
authority store; see
[finding 0012](findings/0012-temporal-and-postgresql-pass-development-conformance-not-performance.md).

## Responsibility boundaries

Temporal preserves durable procedure: Workflow state and ordering, Activity task
delivery/retry, timers, waits, and recorded completion. It does not own a child
process, a Git worktree, a destination API, or an application claim merely
because an Activity invoked them.

The application store is authoritative for logical session identity, the current
owner generation/token, process registration, accepted outcome, and event/effect
evidence. Every mutation that must reject a stale writer carries the generation
and token to this boundary.

An executor starts as `launch_pending`; only successful process registration
makes it `running`. A retry may attach to `running` work. `launch_pending` alone
does not justify either attachment or replacement: live evidence now shows it
can describe both a pre-`exec` phantom and a post-`exec` child blocked before
registration. A trusted discovery/control plane can support attachment to the
exact child. An explicit replacement policy instead creates a higher fenced
generation and marks the old claim `superseded`.

Replacement also requires the incoming Temporal attempt to exceed the active
executor's attempt, preventing a delayed old attempt from reclaiming authority.
Progress, effects, and first completion require `running` state in addition to a
valid generation/token. These are application rules because Temporal cannot
observe the boundary between a store commit and OS process creation.

The agent simulator is a separate OS process. It emits progress, attempts an
external effect, produces an outcome, can block at named barriers, and can outlive
the Worker that launched it. A PID is diagnostic data, not durable identity.

The experiment controller owns fault timing. Named barriers make the dangerous
window causal: a test waits for an observed boundary, injects the fault, and then
releases work. Wall-clock sleeps are not a synchronization contract.

## First-milestone variants

- **Unsafe control:** each Activity attempt launches a new executor and the
  destination accepts unfenced writes. The oracle expects duplicate writers and
  effects; if it passes the safety invariant, the control is invalid.
- **Stable reattachment:** all attempts use one logical session key. A retry
  attaches to a recorded running executor and observes its eventual outcome.
- **Fenced replacement:** replacement is explicit and increments the ownership
  generation. The old token remains observable but no longer authorizes effects
  or completion.

An Activity heartbeat may speed detection and carry recovery hints, but it is not
the creation fence. Retry lookup starts from the stable application session key so
the crash-before-first-heartbeat window remains representable.

## Completion identity boundary

An asynchronous Activity task token identifies a delivery attempt closely enough
for the pinned Temporal Server to reject it after a later attempt starts. A
Workflow ID, Run ID, and Activity ID identify the logical pending Activity, not
the external executor submitting the request. Completion by logical ID therefore
requires an application authorization check when old and new executors may
coexist.

The completion-identity experiment uses a monotonic attempt plus an opaque owner
capability. Only the capability hash is persisted. A newer attempt atomically
replaces the active hash; an older capability and a different competitor for the
same attempt fail closed. This is a lab mechanism, not a claim that authorization
and a remote effect are atomic.

## External-effect boundary

Temporal's Activity state machine and the destination's mutation state machine
have no shared transaction. The experiment makes the gap explicit:

```text
destination mutation committed
        ↓ exact barrier
Worker SIGKILL
        ↓ Start-to-Close timeout
Activity attempt 2
        ↓
one Temporal completion; one or two destination effects
```

The Activity carries a stable logical effect ID across retry. What that ID can
do depends on the destination. An idempotent API and the lab's simulated message
destination atomically deduplicate it; a database makes it a unique transaction key; the Git and
non-idempotent API arms reconcile before repeating; the artifact store publishes
a content-addressed blob and stable reference. The common harness records the
same boundary, identities, and oracle but does not flatten these mechanisms into
a fictitious generic exactly-once interface.

Reconciliation is weaker than atomic destination deduplication. The current
non-idempotent API and Git conclusions require serialized same-ID callers. The
large-artifact experiment now covers the gap between blob publication, pending
reference creation, durable reference publication, Activity completion, and consumer
acknowledgement. Its content-addressed protocol rejects conflicting references and
explicitly removes unreachable blobs and pending records. This remains a single-host,
sequential filesystem result; remote-store consistency and concurrent collection are
separate boundaries.

## Deliberate limitations

The first milestone is single-host. The store can name a surviving local process;
cross-host reachability and routing remain separate experiments. The first
synthetic effect destination understood fencing. The external-effect matrix now
includes weaker destination semantics and preserves the assumptions and remaining
ambiguity for each mechanism.

The launch-gap experiments cover both sides of `exec`. Their central result is
that durable launch state is not process truth. Historical post-`exec` evidence
uses a trusted loopback barrier as discovery evidence. Current Codex experiment
code can instead pre-register the exact point/session/generation/actor tuple and
authenticate every HTTP barrier arrival with a random per-run credential passed
through inherited file descriptors. The credential and request authenticator are
not evidence fields. This authenticates the local fault boundary; it does not
turn the barrier into process truth, provide cross-host discovery, revoke copied
provider credentials, or clean up an uncooperative child.
