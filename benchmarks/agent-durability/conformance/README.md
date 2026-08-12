# Coding-agent durability conformance apparatus

This command checks the deterministic machinery used to admit coding-agent
durability evidence. It is deliberately narrower than integration conformance:
it captures no Temporal Event History and does not exercise the portable
`turn_id`, `operation_id`, `effect_id`, or owner-capability binding. A live
adapter must supply those records and replay every captured history before it
can claim conformance to the product protocol.

## Question and invariant

The question is whether the shared schedule, append-only writer, invalid
controls, and independent oracle can distinguish the four registered failure
boundaries without using adapter logs as verdicts.

The apparatus is conformant only when every case has one unfaulted
`valid-pass`, three unsafe `valid-fail` trials, three protected `valid-pass`
trials, and all four invalid controls are rejected for their intended reason.
The final report is binary. It contains no score, rate, latency, percentile, or
confidence field.

## Failure boundaries and oracle

The four cases reuse `adl.cross-system.v1`:

- a surviving executor after registration;
- an external effect committed before step completion;
- a replacement generation committed before stale actions; and
- cancellation committed before an unreachable process resumes.

Faulted trials use the exact named sequence barriers already implemented by
the calibration harness. The malformed, missed-boundary,
wrong-process-identity, and contradictory controls are derived from sealed
protected evidence, published under unique identities with the manifest last,
and retained after rejection. The aggregate oracle reloads the raw files,
recomputes every legacy verdict, compares it with the exclusively stored
verdict, and verifies the exact expected inventory.

## Run

Choose a new evidence root; the command refuses any existing path:

```bash
make coding-agent-conformance \
  EVIDENCE_ROOT="$PWD/benchmarks/agent-durability/conformance/evidence/<new-suite-id>"
```

The 2026-08-11 development run is preserved at
[`evidence/calibration-20260811-v3`](evidence/calibration-20260811-v3). Its
[`conformance-report.json`](evidence/calibration-20260811-v3/conformance-report.json)
is `conformant`: 16 unfaulted/protected passes, 12 distinguishing unsafe
failures, and four rejected invalid controls. Each replay disposition is
explicitly `not_applicable` because the calibration emits a native journal, not
a Temporal history. The superseded v1 and v2 roots are retained unchanged. V1
pinned an ignored build artifact without retaining those executable bytes. V2
added preservation; final review then added fail-closed report and inventory
validation. V3 preserves the exact content-addressed executable under
`inputs/`, and both the oracle and final writer rehash it.

Run the pinned race-and-coverage gate with:

```bash
make coverage-coding-agent-conformance
```

## Responsibility split and falsifier

The profile fixes the schedule and binary admission rule. The legacy writer
owns exclusive raw publication, the independent oracle owns verdicts, and the
schema manifest pins the portable structural contract. An integration remains
responsible for full logical identities, authority binding, destination
observations, actual process/service faults, and Temporal history replay.

The apparatus result is falsified if an unsafe arm passes, a protected or
unfaulted arm fails, an invalid control is admitted or rejected without its
intended reason, stored and recomputed verdicts differ, evidence is overwritten,
schema or executable bytes differ from their pins, or a score-like field enters
the report. It supplies no evidence that a particular coding-agent integration
survives recovery.
