# Finding 0018: Topology measurement admission is independent before pilot

## Observation

A full correctness, determinism, replay, race, evidence, and statistical review
found that the v5 apparatus result was behaviorally distinguishing but still
trusted several derived fields too much. The review was authorized to fix
in-scope findings and produced RED tests before each correction.

The pilot scheduler had balanced three odd slots only within each stratum. Its
fixed odd-slot choice made Direct Activity first in 176 of 264 pilot pairs and
Child Workflow first in 88. The corrected frozen-seed algorithm assigns the
extra first position across strata, producing 132/132 globally while retaining
a 2/1 split within every stratum. Publication schedule bytes and the registered
publication hash remain unchanged.

The admission oracle now reconstructs every registered semantics and recovery
metric from causal events, dependency requests, destination actions, item
lifecycle records, and parsed native Temporal history. It no longer trusts
self-reported event counts, Workflow-task counts, history bytes, latency/load
metrics, recovery roles, poison designation, or item cost totals. Peak recovery
QPS uses the registered ten-millisecond sliding window. Native-history byte
counts use canonical compact JSON, exact preregistration bytes are SHA-256
pinned, and a synthetic fixture envelope must declare `fixture: true`; live
matrix runs reject such an envelope.

The process-backed suite exposed one further RED case after those changes. A
generic `recovery_observed/observed` event immediately after fault injection was
being interpreted as silent-progress deadline detection. The corrected oracle
uses the authority revocation when the protected procedure fences the wedged
owner and falls back to `recovery_observed/failed` for the unsafe deadline
miss. Both event shapes have regression coverage.

The accepted append-only root is
[`matrix-20260809-v7`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v7).
It retains the complete 88-stratum, 3,520-block schedule; 88 fixture pairs;
four invalid controls; and 23 live Temporal sentinel pairs. All 38 unsafe
fixture arms and all 38 unsafe live arms distinguished. All 138
protected/unfaulted fixture arms and all eight protected/unfaulted live arms
passed correctness, safety, liveness, and diagnosability. All four invalid
controls were rejected and all 46 live histories replayed.

A fresh disk-only audit and an explicit replay test independently revalidated
v7 after generation. The root has 3,799 files; its final inventory seals the
other 3,798 artifacts totaling 123,887,544 bytes. The harness digest is
`a710d8378eedec5398b6333a4f6e9554e3811f371e74a5d30048cfc297a1f4a2`,
the agent digest is
`308b92c8878e0085ff917118b0b9ca6ed747ffe40bba4f503b039c1adc7ba4d8`,
and the Temporal CLI digest is
`fcbcc8c64aec1b8cc1d4268891bf2bd0ec15eaccfe110c055ad1950f48d43452`.
Fresh `-trimpath` builds reproduce the two repository executable digests.
The repository-wide race suite passed after the review, and the final
topology-specific coverage profile reported 82.8% combined statement coverage.
`go vet`, `staticcheck`, `golangci-lint`, module verification, and scoped secret
scans also passed.

[`matrix-20260809-v5`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v5)
remains unchanged and is superseded for current-source claims. The empty
[`matrix-20260809-v6`](../../benchmarks/agent-durability/topology/evidence/matrix-20260809-v6)
root is also preserved with `failure.json`: an incorrect preregistration path
failed at preflight before any episode executed. V7 used the corrected path and
a new root rather than reusing or hiding that failure.

## Inference

The topology apparatus is ready for the preregistered pilot with a stronger
measurement boundary than v5: a stored derived metric cannot change the verdict
unless its raw evidence changes consistently, pilot arm order has no global
2:1 imbalance, and live history cannot be substituted with a synthetic event
count. These properties make later latency, load, retry, and history comparisons
auditable rather than assertions made by the arm under test.

This remains apparatus evidence, not comparative performance evidence. The 88
fixture pairs and 23 selected live pairs are publication-excluded controls and
sentinels, not independent randomized pilot or publication episodes. They do
not estimate a topology effect, and no exactly-once guarantee is inferred.

## Responsibility boundary

Temporal supplies durable Workflow, Child Workflow, Activity, timer, retry,
cancellation, version marker, and replayable history behavior in live runs.
Application code supplies stable identity, fixed membership, generation and
capability fencing, shared budgets, recovery roles, terminal accounting, and
the destination protocol. External services expose physical dependency
requests and authoritative effects. The harness supplies frozen scheduling,
exact barriers, negative controls, raw-evidence reconstruction, canonical
history accounting, provenance binding, append-only storage, and disk audit.

## Falsifier

This finding is false if any registered metric can be changed without a
corresponding raw-evidence change and still be admitted; if a live run can use
a fixture history; if pilot first-arm counts are not 132/132 globally; if a
generic recovery observation is treated as deadline detection; if any v7
inventory, matched input, verdict, history replay, or executable digest fails;
or if these publication-excluded episodes are used to estimate topology cost.
