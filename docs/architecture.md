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

An executor starts as `launch_pending`; only successful process registration
makes it `running`. A retry may attach to `running` work. Recovering an
unregistered pending launch instead creates a higher fenced generation and marks
the old claim `superseded`. Replacement also requires the incoming Temporal
attempt to exceed the active executor's attempt, preventing a delayed old attempt
from reclaiming authority. Progress, effects, and first completion require
`running` state in addition to a valid generation/token. These are application
rules because Temporal cannot observe the boundary between a store commit and OS
process creation.

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

The current launch-gap result covers Worker death before `exec`. Death after
`exec` but before the child registers remains ambiguous: generation fencing can
reject the old child, but process discovery and cleanup are not yet established.
