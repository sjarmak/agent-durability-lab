# Finding 0002: A durable launch decision is not process liveness

**Status:** observed in a final v3 preserved pair plus two automated final-protocol
live trials per arm; two earlier preserved pairs remain as pre-review evidence

**Versions:** Go 1.25.12; Temporal Go SDK 1.47.0; Temporal CLI 1.8.0; Temporal
Server 1.31.2; Linux amd64

## Claim

After a Worker dies between the application's durable launch decision and the
OS process start, Temporal retries the Activity but cannot determine whether the
external agent exists. The control proves that attempt-2 application code can
attach to a PID-less phantom. Because that code has no terminal condition until
an application outcome appears, indefinite waiting is an inference from the
observed state plus the wait-loop implementation—not a measured duration.

Application-level launch lifecycle plus fenced conditional replacement closes
this pre-`exec` gap. A retry replaces `launch_pending` work under a higher
generation only when its Activity attempt is newer, but attaches to `running`
work that has registered a process.

## Observations

In the final [v3 control](../../experiments/worker-death/evidence/launch-gap-20260806-v3-control/application-state.json),
generation 1 remained `launch_pending` with no PID, process identity, effect, or
outcome. Attempt 2 recorded `activity_reattached` to that generation. The harness
recorded `phantom_launch_observed` and only then canceled the Workflow. Its
verdict is valid and records `invariant_satisfied: false`.

In the final [v3 recovery](../../experiments/worker-death/evidence/launch-gap-20260806-v3-fenced-recovery/application-state.json),
attempt 2 atomically changed generation 1 to `superseded`, created generation 2
as `launch_pending`, registered one generation-2 process, accepted one effect,
and accepted one outcome. The Workflow result equals that application outcome.
The v1/v2 pairs reproduce the same visible states but predate the reviewed
attempt-order, registration-gate, and barrier-identity mechanisms; they remain
preserved and are not used to claim those mechanisms.

The recovery histories contain one compacted Activity-start event with
`attempt: 2` and attempt 1's heartbeat timeout in `last_failure`. The canceled
control histories retain the logical Activity schedule and cancellation but not
an Activity-start event; the application event journal is the evidence that
attempt 2 reattached. This is an observed difference in exported history shape,
not evidence that the control did not retry.

The protocol test also delays an obsolete process until after pending-launch
replacement. Its generation-1 registration receives `ErrStaleOwner` before the
simulator can record progress or reach an effect barrier. A separate store test
proves that a later retry with the same recovery policy attaches, rather than
replaces, after generation 2 reaches `running`. Additional tests reject a
delayed attempt 2 after attempt 3 owns the pending launch and reject progress,
effects, or initial completion while the active executor is not `running`.

## Responsibility split

- Temporal supplied the stable logical Activity execution, heartbeat timeout,
  and attempt-2 delivery.
- The application store supplied `launch_pending` versus `running`, atomic
  conditional replacement, monotonic attempt validation, generation advancement,
  registration gating, and stale registration rejection.
- The Activity supplied the stable session ID and selected the recovery policy.
- The OS supplied process start and identity only after the application had
  already made its durable launch decision.

Temporal procedure durability did not imply external-process liveness.

## What this does not establish

- This finding alone does not cover a child that started immediately before
  Worker death but failed to register; [finding 0005](0005-launch-pending-does-not-identify-process-reality.md)
  now supplies that separate evidence.
- Whether Temporal durably observed a heartbeat from the attached control retry.
- Cross-host discovery or attachment to an unregistered process.
- Automatic policy for how long a `launch_pending` state may remain valid.
- Cleanup or credential revocation for an OS process rejected during registration.
- Safety at a destination that ignores the generation fence.

The post-`exec`, pre-registration window was a separate ambiguity at the time of
this finding. [Finding 0005](0005-launch-pending-does-not-identify-process-reality.md)
now observes trusted local discovery, attachment, fenced replacement, stale
registration rejection, and cooperative cleanup at that boundary.

An attempted stronger control oracle waited for attempt-2 heartbeat details via
Temporal's Describe API before cancellation. One of two live trials reached the
overall deadline without that exact observation, so the check was removed from
this experiment rather than being reported as evidence. Heartbeat visibility and
attempt compaction need a dedicated reproduction.

## Falsifier

This finding is narrowed or falsified if a repeated control run starts a child
before the recorded boundary, if a control is classified as correct despite the
phantom, if recovery accepts generation-1 registration or effects, if recovery
replaces an already registered live generation, if more than one effect/outcome
is accepted, or if the Workflow and application outcomes diverge.
