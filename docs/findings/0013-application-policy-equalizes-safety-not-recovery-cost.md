# Application policy equalizes safety, not recovery cost

**Status:** supported for the pinned single-host publication population; no scalar winner  
**Protocol:** `adl.publication.v2` / `adl.cross-system.v2`  
**Preregistration:** [`publication-preregistration-v2.json`](../../benchmarks/agent-durability/publication-preregistration-v2.json)  
**Harness freeze:** [`publication-harness-freeze-v2.json`](../../benchmarks/agent-durability/publication-harness-freeze-v2.json)  
**Publication evidence:** [`publication-v2-20260809-v1`](../../benchmarks/agent-durability/evidence/publication-v2-20260809-v1)  
**Corrected analysis:** [`publication-v2-20260809-v1-analysis-v5.json`](../../benchmarks/agent-durability/evidence/publication-v2-20260809-v1-analysis-v5.json)

## Question and invariant

When the same agent procedure runs through Temporal or a PostgreSQL queue, do
application fencing and bounded-recovery policies preserve correctness, safety,
liveness, and diagnosability after ABA authority changes and five recovery
failures? Once outcome parity holds, what queueing, active execution, retry,
recovery, throughput, backlog, durable-evidence, and intervention cost is
observed?

The central safety invariant is that a previously authorized execution cannot
regain authority merely because its owner label becomes current again. Recovery
must also stay within the common retry, concurrency, admission, poison, and
progress policies. Faults occur only after named barrier arrivals. The
independent oracle reads common authority, workload, dependency, destination,
process, and native records; adapter logs do not choose the verdict.

## Population and admission

The frozen schedule produced 720 dispositions: 540 executed primary pairs and
180 predetermined reserves that were not required. Every one of the 18
case/probe strata reached exactly 30 valid pairs with no invalid pair. This is
1,080 valid system executions. All 13,503 population-inventory entries rehashed
to their recorded values; the inventory file SHA-256 is
`6e4e7284e1deae9d3155da9e6bef68279dbc5d9af3be39bfb646e6649af2a743`.
All 540 Temporal histories replayed before evidence admission, all 540
PostgreSQL journals were retained, and no PostgreSQL run remained in `running`
state after teardown.

For each system, all 360 unfaulted/protected executions passed correctness,
safety, liveness, and diagnosability. All 180 unsafe executions distinguished
their control. Each 30/30 rate has a Wilson 95% interval of `[0.886, 1.000]`.
Unsafe controls are deliberately outcome-failing and are not
efficiency-eligible.

## Safety and recovery behavior

The two systems reached the same protected logical behavior because they ran
the same application policies:

| Case | Unsafe observation, median per system | Protected observation, median per system |
| --- | --- | --- |
| ABA reacquisition | 4 stale A/g7 actions accepted after A/g9 became current | 0 stale actions accepted |
| Retry amplification | 16 physical requests for one logical operation | 4 requests, amplification 4, cost 4 |
| Outage recovery | Retry concurrency reached 8 | Retry concurrency limited to 1 |
| Backpressure | All 100 offered items admitted, violating capacity | 80/100 rejected at admission in both systems |
| Poison isolation | 24 requests consumed by the mixed cohort | 14 requests; poison work quarantined and healthy work completed |
| Silent progress | Wedged work was not durably recovered | Wedged owner replaced, legitimate wait retained, 0 stale accepts |

Temporal supplies Workflow history, durable Workflow/Activity scheduling, and
replay. It does not supply the tested generation/capability fence, shared retry
budget, admission rule, poison quarantine, or progress-deadline policy. Those
are application mechanisms. PostgreSQL supplies transactions, row locks, and
durable rows; the queue, generation, retry, outbox, and progress algorithms are
application code. The accepting destination must still enforce the fence.

## Protected-arm efficiency

The table reports medians across 30 matched pairs. `Difference` is
Temporal minus PostgreSQL; brackets are the preregistered 20,000-resample paired
percentile-bootstrap 95% interval for the median pair difference.

| Case / metric | Temporal | PostgreSQL | Difference [95% interval] |
| --- | ---: | ---: | ---: |
| ABA recovery latency, ms | 45.5 | 1 | 44.5 [40, 54.5] |
| ABA end-to-end latency, ms | 98.5 | 4 | 94.5 [89.5, 114] |
| Retry physical requests | 4 | 4 | 0 [0, 0] |
| Retry delay, ms | 1,058 | 5 | 1,053 [1,050, 1,094.5] |
| Retry recovery latency, ms | 2,078.5 | 9 | 2,069 [2,066.5, 2,071.5] |
| Outage recovery latency, ms | 1,200.5 | 211 | 989.5 [979.5, 992.5] |
| Outage backlog area, task·ms | 12,691.5 | 1,234.5 | 11,454 [10,865.5, 11,537] |
| Outage drain p90, ms | 1,114.5 | 186 | 928 [923, 931] |
| Backpressure queue latency, ms | 874.5 | 45 | 829.5 [768, 908] |
| Backpressure end-to-end, ms | 982.5 | 47 | 935.5 [920.5, 1,009] |
| Backpressure throughput, successful requests/s | 25.65 | 1,569.39 | -1,535.25 [-1,567.84, -1,514.67] |
| Backpressure rejection fraction | 0.8 | 0.8 | 0 [0, 0] |
| Poison healthy-task latency, ms | 222 | 16 | 204.5 [164.5, 409.5] |
| Silent-progress detection latency, ms | 68 | 6 | 62.5 [57, 67] |
| Silent-progress recovery latency, ms | 44 | 4 | 40 [37, 65] |

The latency decomposition locates most of the difference outside dependency
service execution. Protected retry active time was 4 ms in both systems;
protected outage active time was 48.5 ms for Temporal and 49 ms for PostgreSQL;
backpressure was 42 versus 43 ms; poison isolation was 15 versus 16 ms; and
silent progress was 1 ms in both. Queueing and retry/recovery delay, not those
active intervals, produced the larger Temporal end-to-end values. ABA is the
exception because its deliberately delayed generation-7 request remains an
active external interval during the ownership cycle.

Protected Temporal evidence was also larger in every case. Median raw durable
record counts ranged from 58 to 644 for Temporal and 28 to 512 for PostgreSQL;
median raw bytes ranged from 46,068 to 482,786 versus 24,445 to 274,346. These
are the benchmark's common evidence plus native history/journal exports, not a
measurement of production database storage. No episode required a recorded
operator intervention.

## Inference and limits

The population resolves the benchmark's safety and liveness question: both
systems compose with the portable policies, and neither native substrate makes
the unsafe controls safe by itself. It also resolves the pinned-host efficiency
question: under Temporal CLI 1.8.0 / Server 1.31.2, Go SDK 1.47.0, PostgreSQL
16.14, Go 1.25.12, eight workers/connections, and these small workflows, the
Temporal arm had higher queue and recovery latency while application-controlled
attempt counts, admission, and active dependency time were usually the same.

This is not a universal product-performance ranking. The workload intentionally
uses `MaximumAttempts: 1` for individual Temporal Activities and expresses the
shared retry policy in the durable procedure, so equal request counts establish
policy parity rather than a claim about default native retries. The run uses one
local development server, one local PostgreSQL instance, no network or disk
isolation, and one fixed concurrency envelope. It does not compare hosted
clusters, tuned deployments, multi-host failover, or operational staffing.

A later within-Temporal question is now frozen separately in the
[`topology contract`](../../benchmarks/agent-durability/topology-contract-v1.json)
and
[`topology preregistration`](../../benchmarks/agent-durability/topology-preregistration-v1.json):
direct per-item Activities versus per-item Child Workflows across fixed fan-out
and the full recovery-dynamics suite. That preregistration is prospective only;
it supplies no implementation or evidence and does not change this finding's
cross-system population or conclusion.

## Analysis correction lineage and falsifier

The first analysis file, [`analysis.json`](../../benchmarks/agent-durability/evidence/publication-v2-20260809-v1-analysis.json),
incorrectly excluded ABA dependency requests whose accepted outcome string is
`accepted`; it therefore reported zero ABA completion latency. Analysis v2
corrected that raw-anchor interpretation. It is also retained, but its
supporting execution span included retry waits. Analysis v3 implements the
frozen decomposition by summing authorized dependency-request intervals.
Analysis v4 adds strict rejection of files absent from the sealed inventory and
records analyzer binary SHA-256
`c6eb96ea5a5ae609eb3aba7396ee25c59b935d533257e0eb5aa6c834f31adb47`;
its estimates are unchanged from v3. Analysis v5 additionally derives every
pair and system evidence directory under the caller-supplied sealed root rather
than following the legacy stored absolute paths. It also regenerates the frozen
schedule from the preregistration and requires an exact match with the sealed
schedule and population record order. Its analyzer binary SHA-256 is
`89136dd3a5502b9f023e9c3a2e9d7f1ee13575463ad36c5db452567feec207af`,
and the analysis file SHA-256 is
`737cd7d596232b4c14412ed999b5dc05454120d7020b2233adb1f73bb791a190`.
After removing generation time and analyzer binary identity, v5 is byte-for-byte
identical to v4. No
raw publication file, primary estimand, admission decision, schedule, or
bootstrap method changed. Backlog area is reconstructed from every outage
failure finish and matching successful retry finish; the legacy oracle's zero
`queue_depth` summary is not used.

The supported conclusion is false if a protected stale action was accepted, an
unsafe control did not distinguish, a claimed valid run lacks a native record,
a Temporal history fails replay, a PostgreSQL generation fence accepts an old
completion, a population or evidence hash differs, or the corrected metrics
cannot be regenerated from the raw anchors. Performance differences must also
be reconsidered under another host, deployment topology, concurrency envelope,
or workload scale.
