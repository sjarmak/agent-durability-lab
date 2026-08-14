# Finding 0005: `launch_pending` does not identify process reality

One `launch_pending` record with no PID has two meanings. No child started, or
a live child has not registered. Temporal history cannot separate them. Three
attach trials and three fenced replacement trials show both policies.

**Status:** observed in six valid final v3 live trials: three attach-control
trials and three fenced-replacement trials; twelve earlier live trials remain as
pre-review evidence and are excluded from the final claim

**Versions:** Go 1.25.12; Temporal API 1.63.4; Temporal Go SDK 1.47.0;
Temporal CLI 1.8.0; Temporal Server 1.31.2; Linux amd64; agent protocol
`worker-death-v4`

## Claim

The same durable application state—one generation-1 `launch_pending` executor
with no PID—can mean either that no child was started or that a child is already
alive but has not registered. Temporal Activity retry cannot distinguish those
worlds because neither OS process creation nor application process registration
is part of Temporal's Event History.

A retry policy therefore needs evidence beyond the launch claim. If a trusted
discovery mechanism proves the exact child alive, retry can attach without
launching a competitor. If policy instead replaces the pending launch, the
application must advance a fence before starting the replacement and reject the
old child's delayed registration. Neither choice follows from `launch_pending`
alone.

## Failure boundary and oracle

The generation-1 child completed `exec`, read its private launch request, and
reported its Linux PID/start identity at `before-registration/1`. Activity
attempt 1 separately reported that its launcher had returned and it was still
before its first heartbeat. The controller then preserved
`pre-kill-state.json`, sent `SIGKILL` to Worker 1's exact PID, and proved that the
child's PID/start identity remained alive.

Every final pre-kill snapshot records:

- one active generation-1 executor in `launch_pending`;
- PID 0 and no process-start identity in the application store;
- a distinct live child PID with a non-empty process-start identity;
- the child's owner-token hash matching the active application owner; and
- child barrier arrival before snapshot capture and Worker death.

The final matrix produced six valid verdicts. Representative evidence:

- [attach pre-kill state](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-attach-control-trial-1/pre-kill-state.json) and [final state](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-attach-control-trial-1/application-state.json): attempt 2 attached to generation 1, launched no competitor, and the discovered PID registered, produced the only effect/outcome, and exited.
- [replacement pre-kill state](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-fenced-replacement-trial-1/pre-kill-state.json) and [final state](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-fenced-replacement-trial-1/application-state.json): attempt 2 superseded generation 1 and completed under generation 2; the still-live old child later presented its original PID/start identity, registration failed with `stale_owner`, and that exact process identity disappeared.

The three fenced trials varied in whether the controller's
`stale_child_alive_after_replacement` observation was journaled before or after
generation 2 registered. That concurrent ordering is not part of the claim.
Every trial preserved the required partial orders: replacement preceded the
old-child liveness observation, and replacement completion preceded stale
registration rejection.

Read with [finding 0002](0002-launch-decision-is-not-process-liveness.md), the
result gives two observed external realities for the same durable store shape:
the pre-`exec` control has no process, while this post-`exec` experiment has a
live process.

## Responsibility split

- Temporal supplied the heartbeat timeout and Activity attempt-2 delivery. It
  did not discover, address, attach to, revoke, or terminate the child.
- The application supplied stable session identity, explicit attach/replacement
  policy, monotonic generation replacement, registration fencing, one accepted
  effect/outcome, and the durable event journal.
- The loopback child barrier supplied trusted same-host discovery evidence. It
  is a laboratory synchronization mechanism, not a production process registry.
- Linux supplied PID/start identity and the observed survival/exit boundary.

The application fence revokes only the generation capability at the cooperative
work-store API. Temporal and this experiment do not revoke arbitrary credentials
already held by the process, and a destination that ignores the fence can still
accept a stale effect. Detached children inherit no Worker environment in this
lab; raw owner tokens stay in mode-0600 local state and do not appear in portable
evidence.

## Preserved pre-review evidence

The six v1 directories recorded successful arm behavior but mislabeled the
agent protocol as `worker-death-v3`. The six v2 directories corrected the
protocol label but did not preserve the standalone pre-kill store/process
snapshot. They remain append-only evidence of the research sequence, but only
v3 supports the complete boundary claim above.

## Scope — what this does not show

- Cross-host discovery, routing, or termination of an unregistered child.
- A general rule for when to attach versus replace, or a lease duration after
  which a live but wedged child should lose ownership.
- Containment of a compromised same-user process that can bypass the work-store
  API or read local laboratory state.
- Revocation at Git, API, message, artifact, or other real destinations.
- Cleanup after an uncooperative process ignores registration rejection.
- Behavior on non-Linux process identity implementations or different Temporal
  versions.

## What would change this conclusion

This finding is narrowed or falsified if a fresh pinned-version run lacks the
child at the captured boundary, records a process in the store before the kill,
allows attach to create another executor, accepts generation-1 registration or
effects after replacement, leaves more than one accepted effect/outcome, returns
a Workflow result different from the application outcome, or cannot observe the
obsolete PID/start identity disappear after cooperative rejection.
