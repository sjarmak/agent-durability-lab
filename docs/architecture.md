# Laboratory architecture

The initial architecture is a hypothesis to test, not a universal prescription.

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

## Responsibility boundaries

Temporal preserves durable procedure: Workflow state and ordering, Activity task
delivery/retry, timers, waits, and recorded completion. It does not own a child
process, a Git worktree, a destination API, or an application claim merely
because an Activity invoked them.

The application store is authoritative for logical session identity, the current
owner generation/token, process registration, accepted outcome, and event/effect
evidence. Every mutation that must reject a stale writer carries the generation
and token to this boundary.

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

## Deliberate limitations

The first milestone is single-host. The store can name a surviving local process;
cross-host reachability and routing remain separate experiments. The synthetic
effect destination understands fencing; later experiments must use destinations
with weaker semantics and preserve their ambiguity rather than hiding it.
