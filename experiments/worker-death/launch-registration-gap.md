# Launch decision / process registration gap

## Question

What should an Activity retry do when the application store contains a durable
launch decision but no process has registered for it?

## Invariant

A retry must not treat an unregistered launch claim as proof that an agent is
running. Recovery must eventually produce one accepted effect and one accepted
outcome, and any obsolete launch authority must fail closed.

## Exact boundary

Attempt 1 commits `executor_launch_decided`, reaches the
`activity-after-launch-decision/1` barrier, and has not called the process
launcher. The harness records the pending executor and then sends `SIGKILL` to
the Worker. No child process exists at this boundary.

The barrier is a protocol event, not a timing delay. A run is invalid unless the
pre-kill snapshot contains generation 1 in `launch_pending` state with no PID or
process-start identity.

## Arms and success criteria

| Arm | Retry policy | Expected observation |
| --- | --- | --- |
| control | Attach to any existing session | Attempt 2 attaches to generation 1, but no process, effect, or outcome can appear. The harness records the phantom state before canceling the Workflow. The application invariant is false. |
| fenced recovery | Replace only an unregistered pending launch | Attempt 2 creates generation 2, marks generation 1 superseded, launches one process, and returns the generation 2 outcome. Exactly one effect and outcome are accepted. |

The recovery policy must not replace a registered `running` executor. That case
is reattachment, not abandoned-launch recovery. It must also reject a replacement
request whose Activity attempt is not newer than the active executor's attempt.

## Responsibility split

- Temporal preserves the Activity retry and the stable Activity ID. It does not
  know whether the application launch decision corresponds to an OS process.
- The work store records the launch lifecycle and atomically replaces a pending
  owner with a higher generation only for a newer Activity attempt.
- The agent protocol registers before progress or effects. Generation fencing
  rejects a delayed process from the superseded owner, and the store rejects
  progress/effects/completion until the active process is `running`.
- The OS and destination systems do not provide this ownership protocol.

## Falsifiers

The conclusion is false if any repeated trial shows one of these observations:

- the control produces a process despite the pre-launch kill boundary;
- the control is reported healthy merely because the retry heartbeats;
- recovery attaches forever to the pending generation;
- recovery replaces an already registered live generation;
- a delayed older Activity attempt replaces a newer pending generation;
- generation 1 can register, write, or complete after generation 2 replaces it;
- more than one effect or outcome is accepted; or
- the Workflow result differs from the application store's accepted outcome.

## Scope limit

This experiment closes the known pre-`exec` gap. A Worker death after the OS
starts a child but before registration is a distinct ambiguity. Fencing makes a
replacement safe from stale application writes, but process discovery and
cleanup at that later boundary require a separate experiment.
