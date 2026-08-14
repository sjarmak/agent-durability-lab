# Finding 0017: The topology matrix is ready for pilot, not publication

The v5 topology apparatus included 88 fixture pairs and 23 live sentinel pairs,
distinguished every unsafe arm, and rejected four corrupted controls. Review
later found it trusted several derived fields. Superseded by Finding 0018.

This finding records the v5 apparatus result. It is superseded for
current-source claims by [finding 0018](0018-topology-measurement-admission-is-independent-before-pilot.md),
which preserves the review defects and binds the corrected v7 root.

## Observation

The accepted append-only integrated root
[`matrix-20260809-v5`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v5)
admits the complete frozen topology apparatus before any independent pilot
episode is counted. Its stored schedule contains exactly 88 strata, 3,520
blocks, 2,640 primary pairs, 880 reserve pairs, and 5,280 primary arm
executions. Direct Activity is first in 1,320 primary and 440 reserve pairs;
Child Workflow is first in the other 1,320 and 440. Every stratum has exactly
30 primary and ten predetermined reserve slots.

The executed apparatus subset contains one deterministic pair from every
stratum, four corrupted controls, and 23 predetermined live Temporal sentinel
pairs. The 88 fixture pairs produced 176 valid arms: all 38 unsafe arms failed
safety and all 138 protected or unfaulted arms passed correctness, safety,
liveness, and diagnosability. Missing lineage, replay incompatibility, a false
recovery prohibited-action count, and mismatched paired input were each
rejected as invalid rather than interpreted as logical outcomes.

The live sentinels produced 46 valid arms. All 38 unsafe arms distinguished;
the eight protected or unfaulted arms passed all four outcome dimensions; and
all 46 parent histories plus every actual Child history replayed. The harness
digest in every live effective input and replay record matches the conformance
binary digest `2a2c149f0554a8f948c3b97dec0b508dfc86771342f0b2fdf586abda86d7fab4`.
The agent digest is
`308b92c8878e0085ff917118b0b9ca6ed747ffe40bba4f503b039c1adc7ba4d8`,
and the Temporal CLI digest is
`fcbcc8c64aec1b8cc1d4268891bf2bd0ec15eaccfe110c055ad1950f48d43452`.

A second disk-only audit ignored the aggregate counts and reconstructed the
schedule, selected blocks, pair membership, matched arm input, raw verdicts,
replay records, and executable provenance. It verified all 3,799 files: the
final inventory seals the other 3,798 artifacts totaling 123,317,052 bytes.
The report marks every fixture and sentinel `publication_excluded` because
none is an independent randomized episode from the preregistered population.

The complete
[`matrix-20260809-v1`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v1)
root remains unchanged but is superseded. Final review after v1 made metric
admission independent of self-reported prohibited-action counts, required an
unsafe arm to fail safety even when correctness also failed, reconstructed
pair data rather than trusting pair JSON, hardened path confinement and bounded
file reads, added a real Temporal readiness RPC, and bound the harness, agent,
and Temporal executables.

The partial
[`matrix-20260809-v2`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v2)
and
[`matrix-20260809-v3`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v3)
roots also remain unchanged and are rejected by their `failure.json` records.
In both, the unsafe fan-out-32 Child-Workflow outage sentinel hit the registered
120-second ceiling and the paired runner correctly recorded
`execution_failed` plus `evidence_missing`. V2 retained 788 scheduled recovery
Activities across the 32 Child histories, with as many as 38 for one child. V3
still retained 697 after Workflow-task concurrency was raised, falsifying that
local-capacity-only correction. The actual missing mechanism was a durable
cohort phase: an early child could retry before every unique child had recorded
its initial outage result.

The corrected common item procedure waits on a separate effect-queue Activity
after the first outage result. That exact gate leaves all eight frozen Work
Activity slots free; the last unique initial Work result records the full
backlog, restores the dependency, and releases the cohort. A Temporal Workflow
version marker lets histories captured before this command change replay the
legacy branch. Current code actively replayed every parent and Child history in
the accepted recovery-v6 root before the accepted integrated run was generated.

The complete
[`matrix-20260809-v4`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v4)
root passed the matrix and disk audit but is superseded for current-source
claims. The subsequent repository-wide race gate observed a normally exited
agent whose procfs state read returned `ESRCH`; the shared probe recognized only
`ENOENT` as gone and therefore made an unrelated live-common run invalid. The
correction treats wrapped `ESRCH` as the same gone disposition, passed focused
unit coverage and five race-enabled live repetitions, and changed both linked
binary digests. V5 is the fresh source-bound matrix after that correction.
The final repository-wide race suite and coverage gate passed after the fix;
the topology production packages, including the integrated matrix package,
reported 82.6% combined statement coverage.

A final provenance regression initially failed because a plain `go build`
does not reproduce binaries built with `-trimpath`. The sealed v5 binaries
already recorded that flag, but the Make targets had not prescribed it. The
three topology evidence targets now use `go build -trimpath`; rebuilding the
matrix harness and agent from the final tree reproduces the recorded v5
digests byte for byte.

## Inference

The frozen schedule, fixture generators, causal oracle, fail-closed pair
runner, live Temporal/process path, replay path, and append-only inventory now
compose across the full semantics and recovery-dynamics suite. The apparatus is
ready for the preregistered three-pair pilot: a pilot failure can now be
interpreted as an observation about an independent episode rather than an
already-known schedule, oracle, or cohort-coordination defect.

This result does not compare topology performance. Deterministic fixtures are
oracle controls, and the 23 live pairs are selected mechanism sentinels rather
than a randomized 8/32/128 population. They cannot estimate latency, attempts,
history growth, tokens, compute, or a cross-topology effect. No publication
slot has been consumed, and no exactly-once guarantee is inferred.

## Responsibility boundary

Temporal supplies durable Workflow, Child Workflow, Activity, timer, retry,
version marker, and replayable history behavior in the live sentinels.
Application Workflow code supplies the common per-item procedure, fixed
membership, stable identity, shared budgets, admission, the versioned outage
cohort gate, terminal accounting, and fencing. The external dependency, work
store, process launcher, and destination expose physical requests and enforce
start-or-attach, authority, and receipt behavior. The matrix harness supplies
the frozen schedule reconstruction, pair execution order, exact barriers,
negative controls, append-only evidence, root confinement, provenance, and
independent disk audit.

## What would change this conclusion

This finding is false if any v5 inventory entry, pair reconstruction, matched
input, verdict, executable digest, or replay fails; if the stored schedule does
not have the exact registered arithmetic and balance; if an invalid control is
admitted; if any unsafe fixture or sentinel passes safety; if any protected or
unfaulted arm fails one outcome dimension; if the two topologies receive
different application policy; or if a pre-gate history cannot replay with
current code. Treating these publication-excluded runs as pilot/publication
episodes or using them for a topology cost conclusion would also falsify the
stated evidence boundary.
