# Temporal and PostgreSQL pass development conformance, not a performance comparison

**Status:** historical development checkpoint; publication completed in [finding 0013](0013-application-policy-equalizes-safety-not-recovery-cost.md)  
**Contracts:** `adl.cross-system.v1`, `adl.cross-system.v2`  
**Temporal evidence:** [`v1`](../../benchmarks/agent-durability/evidence/temporal-v1-20260808-v1), [`v2`](../../benchmarks/agent-durability/evidence/temporal-v2-20260808-v3)  
**PostgreSQL evidence:** [`v1`](../../benchmarks/agent-durability/evidence/postgresql-v1-20260808-v1), [`v2`](../../benchmarks/agent-durability/evidence/postgresql-v2-20260808-v2)

## Question

Can both required systems execute the same durable procedure, expose their
native retry or lease-recovery record, and compose with the common authority,
destination, failure, workload, and oracle protocols without changing the case?

## Observation

For contract v1, each system produced 36 fresh runs: 24 expected passes, 12
distinguishing unsafe failures, and zero invalid runs. The common harness used
real simulator processes and named barriers for every run.

For contract v2, each system produced 54 fresh runs: all 54 were admitted and
diagnosable, all 36 unfaulted/protected runs passed correctness, safety, and
liveness, and all 18 unsafe runs produced their preregistered valid failure.
This includes three trials for ABA reacquisition and every recovery-dynamics
profile.

Temporal 1.8.0 / Server 1.31.2 with Go SDK 1.47.0 executed the durable plan as a
Workflow whose steps are Activities. Each faulted run records the injected
Activity failure and successful retry, and all 54 captured histories replayed
against the current Workflow. PostgreSQL 18.4 executed the plan through queue
rows claimed with `FOR UPDATE SKIP LOCKED`; each faulted run records lease
expiry, generation increment, reacquisition, fenced completion, and a
transactional outcome acknowledgement. A separate live adversarial test proves
that the expired generation cannot complete the reacquired step.

Protected v2 median evidence sizes illustrate native-record representation, not
service throughput:

| Case | Temporal records / bytes | PostgreSQL records / bytes |
| --- | ---: | ---: |
| ABA reacquisition | 138 / 107,086 | 101 / 70,987 |
| Retry amplification | 91 / 84,425 | 62 / 55,461 |
| Outage/backlog recovery | 359 / 345,891 | 326 / 313,400 |
| Backpressure | 653 / 501,973 | 624 / 473,299 |
| Poison isolation | 217 / 207,566 | 188 / 178,847 |
| Silent progress | 103 / 86,271 | 66 / 50,414 |

## Inference and responsibility split

Both adapters pass the three-trial development conformance gate. Temporal
supplies Workflow history, durable Activity delivery, and replay. PostgreSQL
supplies transactions, row locks, and durable rows; the queue algorithm, lease
expiry, generation, retry schedule, and acknowledgement protocol are application
code. In both systems, generation/capability fencing, retry budgets, admission,
poison isolation, progress policy, and destination acceptance remain
application or destination mechanisms.

The common recovery workload currently uses a deterministic scenario clock so
the latency, QPS, backlog, and cost values are deliberately identical across
system bindings. Native history/journal size is observable, but this population
cannot resolve comparative service latency, throughput, or outage-drain
behavior. Three development trials also do not satisfy the preregistered 30-run
publication population. Therefore no system winner, confidence interval, or
performance claim is reported.

## Falsifier and unresolved publication work

The conformance conclusion is false if history replay fails, a Temporal faulted
run lacks the retry failure, a PostgreSQL journal lacks lease reacquisition, an
expired generation completes, common and native identities disagree, or any
protected run fails an outcome dimension. A publication comparison additionally
requires a system-timed concurrent workload, randomized paired ordering, 30
fresh independent episodes per arm, recovery curves, and uncertainty analysis.
Those requirements were subsequently satisfied without reusing this
development population; see [finding 0013](0013-application-policy-equalizes-safety-not-recovery-cost.md).
