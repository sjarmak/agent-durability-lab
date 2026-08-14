# Finding 0001: Temporal retry identity does not fence a surviving agent

Temporal redelivered the Activity after Worker `SIGKILL`. The application
protocol decided what happened next. The unsafe arm ran two agents. The
reattach arm reused one. The fenced arm replaced generation 1 and rejected its
late writes.

**Status:** observed on one preserved run per arm plus two automated live trials
per arm

**Versions:** Go 1.25.12; Temporal Go SDK 1.47.0; Temporal CLI 1.8.0; Temporal
Server 1.31.2; Linux amd64

## Claim

After Worker `SIGKILL` before the first Activity heartbeat, Temporal redelivered
the logical Activity as attempt 2. Whether that retry created a competing agent,
reattached to the original, or rejected an old writer was determined by the
application protocol—not by Temporal's Activity retry alone.

## Observations

The [unsafe control](../../experiments/worker-death/evidence/milestone1-20260806-v3-unsafe/application-state.json)
recorded two launch decisions, two process identities, two accepted effects, one
accepted outcome, and one terminal completion rejection. The safety invariant
failed as the negative control required.

The [reattachment arm](../../experiments/worker-death/evidence/milestone1-20260806-v3-reattach/application-state.json)
recorded Activity attempt 2 attaching to generation 1. The child retained the
same PID/start identity after Worker 1 died. One executor produced one effect and
one outcome.

The [fenced arm](../../experiments/worker-death/evidence/milestone1-20260806-v3-fenced/application-state.json)
recorded explicit generation-2 replacement. The replacement outcome was accepted
at event 15. The delayed generation-1 effect was rejected at event 16 and its
completion at event 18. The accepted outcome remained generation 2.

All three Temporal histories contain one logical Activity ID. Server 1.31.2's
export is compacted: it contains one Activity schedule and one Activity-started
event whose `attempt` is 2 and whose `last_failure` records attempt 1's heartbeat
timeout, followed by completion. A fixture-shape test protects this distinction.
The reattachment history replays against current Workflow code.

All v3 verdicts also record matching Workflow and application outcomes. The
original and v2 evidence remain preserved; the original oracle omitted this
cross-boundary comparison, and v2 predates the isolated child environment and
patched Go toolchain. Neither is the basis for the final claim.

## Explanation

Temporal did not directly detect or adopt the child. It detected missing Activity
heartbeats and retried procedure. A stable application session key made two task
attempts converge on one external process. Explicit generation advancement and a
destination-side token check revoked the older process's authority.

Temporal Server source separately validates the attempt embedded in a normal
Activity task token. That protects Temporal completion of an obsolete Activity
task; it does not protect application mutations by a child that has no task token.
The two fences must not be conflated.

## Scope — what this does not show

- Fencing an arbitrary API, Git repository, or message destination that cannot
  validate the application token.
- Safe cross-host attachment.
- Correct automatic replacement of an unreachable or wedged child.
- Cancellation after Worker death.
- The lost-completion window after an external effect has already succeeded.
- `CompleteActivityByID` attempt identity; source inspection suggests a different
  validation path and it needs a dedicated live experiment.

## What would change this conclusion

A repeated safe-arm run that launches a second child without explicit
replacement, loses the original child with the Worker, accepts generation 1 after
generation 2, changes the terminal outcome after stale completion, or fails
current-history replay narrows or falsifies this finding.
