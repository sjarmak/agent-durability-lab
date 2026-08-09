# ABA fencing and recovery-dynamics apparatus distinguish unsafe controls

**Status:** apparatus finding, not a durability-system comparison  
**Contract:** `adl.cross-system.v2`  
**Evidence:** [`calibration-v2-20260808-v1`](../../benchmarks/agent-durability/evidence/calibration-v2-20260808-v1), [`live-aba-v2-20260808-v1`](../../benchmarks/agent-durability/evidence/live-aba-v2-20260808-v1)

## Question and invariant

The authority invariant is that an A/generation-7 request remains obsolete after
B/generation-8 completes and owner label A becomes current again as
generation 9. The recovery-dynamics invariants require bounded retry,
restoration load, admission, poison capacity, and progress recovery while all
accepted work remains accounted for.

## Failure boundaries and controls

The live ABA harness blocks A/generation 7 inside the loopback destination
before authorization. It observes that exact barrier, commits B/generation 8
and A/generation 9, obtains generation 9's outcome and acknowledgement, and
only then releases the old request. The unsafe destination checks only the
recurring owner label; the protected destination atomically checks owner,
generation, and opaque capability.

The deterministic recovery suite uses named controller events and exact fault
brackets for the first accepted retry request, outage backlog target,
offered-load gate, poison release, and silent-progress barrier. Its unsafe arms
respectively multiply four retry layers, synchronize restored work, admit past
capacity, exhaust shared capacity with poison retries, and treat a live process
as sufficient progress.

## Observation

- All 54 calibration runs were admitted and diagnosable: three trials for six
  cases under unfaulted, unsafe, and protected probes.
- All 36 unfaulted/protected calibrations passed correctness, safety, and
  liveness. All 18 unsafe calibrations produced the preregistered valid failure.
- In three live unsafe ABA trials, all four delayed generation-7 actions
  (effect, completion, acknowledgement, and stop) were accepted after A/g9 was
  current, and current-owner liveness failed.
- In three live protected ABA trials, all four delayed actions were rejected,
  the generation-9 outcome and acknowledgement remained accepted, and all four
  outcome dimensions passed.
- The live evidence records three real PID/start identities per run. Rehashed
  boundary, causal, and process-identity contradictions fail closed in the
  independent oracle.

## Inference and responsibility split

The apparatus can distinguish owner-label locking from genuine generation and
capability fencing, and it can distinguish each protected recovery policy from
its intended negative control. This does not establish a Temporal or PostgreSQL
result. The harness supplies exact barriers and evidence; application code
supplies generations, budgets, admission, isolation, and progress policy; the
destination supplies atomic rejection at its accepting boundary. A durability
system must still demonstrate procedure recovery and export its native record
through the same contract.

## Falsifier

This conclusion is false if an unsafe control passes, a protected probe accepts
an obsolete action or exceeds its bound, a valid invariant violation is hidden
as invalid evidence, a malformed boundary or identity is admitted, or a future
required adapter cannot reproduce the common case without changing its input,
oracle, or protected mechanism.
