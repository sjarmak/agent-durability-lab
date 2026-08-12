# Finding 0006: Temporal cancellation does not revoke a detached agent

**Status:** observed in 24 valid final live trials: three trials for four
scenarios under both Activity `WaitForCancellation` policies

**Versions:** Go 1.25.12; Temporal Go SDK 1.47.0; Temporal CLI 1.8.0;
Temporal Server 1.31.2; Linux amd64; agent protocol `cancellation-v1`

## Claim

Canceling a Temporal Workflow does not revoke the authority of a detached agent
process. In all six Temporal-only controls, the Workflow closed as canceled and
the still-live agent subsequently committed one effect and one outcome.

The minimal safety mechanism is an application-owned terminal transition. It
records the cancellation, revokes the active generation, rejects later
registration/progress/effect/completion, and prevents replacement in one work-
store transaction. Process stopping is a separate best-effort operation against
the exact session, generation, owner hash, PID, process-start identity, and
process group. Delivery and acknowledgement are recorded separately.

Across all 18 safe live runs, no effect or outcome was accepted after
cancellation. The agent leader durably acknowledged the cancellation, both the
leader and its spawned tool child recorded stop reception, and both exact
process identities disappeared. Six of those runs killed Worker 1 with
`SIGKILL` before cancellation; Worker 2 performed the disconnected cleanup.
Six froze the complete process group before cancellation; revocation committed
and stop delivery was recorded while acknowledgement remained absent, then the
tree acknowledged and exited only after exact resume.

## Wait policy is procedure, not revocation

All twelve `WaitForCancellation=false` final histories contain one Activity
cancel-request event and no Activity-canceled event. All twelve `true` histories
contain one Activity-canceled event. Safe runs under both policies schedule and
complete the disconnected cleanup Activity before the Workflow closes as
canceled.

That zero-versus-one count describes the preserved population, not a required
absence guarantee. On 2026-08-09, a race-instrumented maintenance gate observed
one prompt Activity-canceled event in a `false` healthy-safe arm. The corrected
oracle permits zero or one when the Workflow does not wait. For current
wait-enabled runs it requires one terminal cancellation observation:
Activity-canceled in healthy and frozen scenarios, and either Activity-canceled
or Activity heartbeat-timeout after Worker death. A loaded ordinary-CI gate on
2026-08-11 exposed the latter race: Workflow cancellation was requested after
Worker 1 died, the dead Activity could not acknowledge it, and its heartbeat
timeout became terminal before Worker 2 cleanup. Current Workflow code treats
the canceled Workflow context as authoritative, performs disconnected cleanup,
and returns cancellation; replay-versioned legacy histories retain their prior
behavior. These maintenance corrections change no preserved run or
application-safety conclusion and do not constitute a new admitted population.

This changes what Temporal waits to observe. It does not change whether the
detached process retains application authority. The six controls violate the
invariant under both policies; the eighteen safe runs satisfy it under both.

A preserved development run adds a narrower warning. With
`WaitForCancellation=true`, Activity attempt 1 was blocked before its first
heartbeat. Its context never observed cancellation, heartbeat timeouts exhausted
the retry policy, and the Workflow failed rather than closing as canceled. The
final wait-enabled arms continue heartbeating. This is observed behavior on the
pinned versions, not a claim that every Activity implementation has this exact
failure shape.

## Terminal race and stale stop

The application store uses first-durable-terminal-transition-wins. The tests
exercise both orders: an accepted outcome makes cancellation return
`already_completed`; a committed cancellation makes the same executor's later
effect and completion return `session canceled`. Repeated cancellation returns
the original terminal record.

OS process control is generation-specific rather than "stop whatever is
running." A Linux process test launches old and replacement trees, delivers a
delayed generation-1 stop, observes both old identities disappear, and proves
both generation-2 identities remain alive. Another negative control signals
only the leader through a pidfd and proves its tool child survives; the
process-tree arm adds isolated process-group signaling and observes both exit.

## Responsibility split

- Temporal records the Workflow cancellation request, Activity cancel request,
  configured wait behavior, cleanup Activity procedure, and final Workflow
  cancellation in Event History.
- The application owns the terminal revocation transaction, first-commit-wins
  race, idempotency, exact executor target, delivery/acknowledgement distinction,
  and post-cancel mutation rejection.
- Linux supplies pidfd signaling for exact leader identity, process-group
  signaling for descendants, signal delivery, and observable process exit.
- A real destination must enforce the revoked capability. The work store proves
  only its own boundary; copied Git, API, cloud, or message credentials remain
  outside this claim.

## Limits

Process-group signaling is not a kernel-atomic identity fence for every member.
The controller validates recorded PID/start/group identities and uses a pidfd
for the leader, but the validation-to-group-signal interval remains a same-host
race. Cgroups or a stronger supervisor may be required before claiming hostile
or multi-tenant containment. Cross-host processes, uncooperative processes,
machine reboot, credentials already copied out of the lab, and destinations
that ignore the application fence are untested.

The completion/cancellation orders and delayed stale stop are deterministic
process/store tests, not additional Temporal live arms. No claim is made about
Temporal selecting the application terminal winner.

## Evidence and falsifier

The executable evidence audit requires exactly three final histories and state
snapshots per scenario/policy pair. Representative runs are under
`experiments/cancellation/evidence/cancellation-20260807-v2-*`. The v1 matrix
remains append-only evidence from before the freeze and cleanup paths were
routed through the exact-identity process controller.

The conclusion is narrowed or falsified if a fresh pinned-version safe run
accepts a post-cancel mutation, replacement starts after terminal cancellation,
acknowledgement appears without the exact target receiving a stop, Worker 2
cannot clean up Worker 1's surviving child, a frozen child mutates before
resume, a stale stop reaches generation 2, a claimed process-tree stop leaves a
recorded descendant alive, or a history lacks the scenario-specific wait-policy
terminal shape stated above.
