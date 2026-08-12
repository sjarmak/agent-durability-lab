# Finding 0007: The live common harness distinguishes unsafe controls

**Status:** observed in 36 valid live apparatus trials: three trials for four
cases under unfaulted, unsafe, and protected probes

**Versions:** Go 1.25.12; Linux amd64; contract `adl.cross-system.v1`; live
adapter source
`c78d32fd08e0e6db8bb68d57e6efbbcaf4fa998efa706c39649869a7a7339d79`

## Claim

The cross-system benchmark apparatus can drive the shared simulator, authority
store, effect journal, named barrier, and exact Linux process controller through
all four v1 failure boundaries and produce evidence that the independent oracle
classifies correctly.

In the corrected v2 suite, all 12 unfaulted and all 12 protected trials are
`valid-pass`. All 12 unsafe trials are `valid-fail`. The unsafe reasons appear in
three trials each: competing owner, duplicate physical effect,
stale action accepted, current owner stopped, missing accepted outcome after the
stale stop, and post-cancellation mutation. No run is invalid.

This establishes an adapter conformance fixture. It is not evidence about
Temporal, Restate, DBOS, PostgreSQL, Claude Code, or Codex.

## What is live

Each trial builds and launches the real `agent-simulator` as an isolated process
group. The simulator registers its PID, process-start identity, generation, and
owner capability in the Bolt authority store and blocks at named HTTP barriers.
The harness uses exact PID/start/group targets for kill, freeze, and resume. It
captures the store journal, effect attempts, process observations, and fault
bracket before the separate oracle writes a verdict.

The public evidence writer accepts typed raw records, validates identities,
contiguous sequences, monotonic times, fault bracketing, and safe run paths, then
publishes each file exclusively and hashes it into the manifest. It has no
verdict API. The calibration adapter and live adapter both consume this writer.

## Responsibility split

- The common harness supplies stable session, generation, effect-attempt, and
  process identities; exact barriers; raw evidence capture; and unsafe controls.
- The application store supplies serialized ownership, cancellation, effect,
  and outcome state. Protected arms use reattachment, generation fencing,
  reconciliation, and revocation at those boundaries.
- Linux supplies process identity, process groups, signals, and observable exit.
- The independent oracle checks hashes, cross-record consistency, exact fault
  placement, and case invariants. Adapters cannot write its verdict.
- No durable execution system participates in this suite, so no recovery
  property is attributed to one.

## Limits

The apparatus is single-host and Linux-specific. Bolt is the calibration
authority and effect journal, not a production destination. The protected
ambiguous-effect arm reconciles against that journal; it does not prove an API,
broker, Git host, or database protocol. The surviving-executor case exercises a
harness-level retry decision while the external agent remains live; future
system adapters must kill their actual durable executor at the same named
boundary. Three development trials per arm establish repeatability, not a
publishable failure-rate estimate.

The later
[`coding-agent conformance apparatus`](../../benchmarks/agent-durability/conformance/README.md)
reuses these four case oracles as a product-development gate. Its preserved
deterministic calibration contains one unfaulted plus three unsafe and three
protected episodes per case, along with four retained invalid controls. That
report validates schedule, writer, and oracle behavior only: it captures no
Temporal history and lacks the portable turn, operation, effect, and capability
bindings, so it does not extend this finding to a coding-agent integration.

The first live suite, `live-common-20260807-v1`, is preserved but superseded. It
recorded the generic adapter version `v1` instead of an immutable source
identity. Review added mandatory adapter provenance and regenerated v2; no v1
file was rewritten or deleted.

## Evidence and falsifier

The corrected evidence is
[`live-common-20260807-v2`](../../benchmarks/agent-durability/evidence/live-common-20260807-v2).
It contains 36 append-only run directories, 24 valid passes, 12 intentional
valid failures, and zero invalid runs. Every effective input records the same
adapter source hash named above.

The conclusion is falsified if an unsafe arm passes, a protected or unfaulted
arm fails, a wrong process or missed boundary is scored instead of invalidated,
the adapter can overwrite evidence or write a verdict, native effect identity
disagrees with the destination journal without rejection, a failed run is
discarded, or a fresh source-pinned three-trial suite changes the stated counts.
