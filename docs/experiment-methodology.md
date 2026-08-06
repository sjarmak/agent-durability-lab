# Experiment methodology

Every experiment has a written contract before its implementation or run.

## Contract

**Hypothesis or invariant.** A precise safety/liveness statement over the whole
application, not "Temporal recovers."

**Failure boundary.** The named event immediately before and after fault
injection, plus the process/component being killed, frozen, delayed, or replaced.

**Oracle.** Machine-checkable success and failure conditions, including the
expected failure of the negative control.

**Identities.** Workflow/run/Activity attempt, logical operation/session,
ownership generation/token, Worker, process, effect, and artifact identities that
must appear in evidence.

**Responsibility split.** The Temporal, application, and destination mechanisms
on which the expected result depends.

**Falsifier.** An observable result that would make the conclusion false or
narrower.

## Execution

1. Establish a clean no-fault run.
2. Run the unsafe negative control and prove the oracle catches its violation.
3. Inject the fault only after the barrier reports the exact boundary.
4. Preserve raw ordered events before interpreting them.
5. Run the smallest added mechanism, then repeat concurrency-sensitive trials.
6. Record version/configuration, commands, outcomes, and any invalid trials.
7. Classify each statement as observed, inferred, or unresolved.

Do not replace an awkward result with a repaired run. Preserve both, explain the
harness or design change, and narrow earlier claims when necessary.

## Evidence format

Raw JSONL records are ordered by a store-assigned sequence and include UTC time,
event kind, session, generation, owner token, Worker, process, Temporal attempt,
and structured details. A summary report evaluates named invariants against those
records. Evidence directories are append-only by convention.

Unit tests validate ownership state transitions. Integration tests validate the
durable store and process launcher. Process E2E tests prove the detached child
survives launcher death. A live Temporal test is required for claims about server
timeout/redelivery and retry on another Worker; the SDK testsuite alone cannot
establish those OS/service behaviors.
