# Finding 0014: The topology foundation fails closed before pilot

## Observation

The frozen Temporal topology protocol now has an executable shared foundation.
It deterministically derives all 88 preregistered strata and balanced
publication arm orders, writes complete append-only run and pair inventories,
reconstructs causal lineage independently, and executes a real hermetic process
through an exact named barrier for both topology identities.

Race-enabled adversarial tests reject mismatched arm inputs, missing or changed
lineage, missed and wrong-identity barriers, fault-order errors, evidence outside
the caller-supplied root, replay incompatibility, recorded schedule drift, and
outcome-derived exclusion. A logical correctness, safety, or liveness failure
remains admitted data and does not stop the second arm. Combined statement
coverage for the production foundation packages was 82.3% on 2026-08-09.

## Inference

The harness can enforce the comparison boundary before a pilot: it distinguishes
an invalid experiment from a valid logical failure and prevents the most direct
forms of topology, schedule, lineage, path, and replay contamination. The
real-process test also establishes that both arm identities use the same
detached work implementation and exact barrier service.

This inference comes from application and harness code, not from a Temporal
topology result. The suite has not yet run the Direct-Activity or Child-Workflow
orchestration cases against captured Temporal histories, so correctness,
safety, liveness, diagnosability parity, and efficiency remain unknown.

## Evidence

The reproducible acceptance evidence is in
[`benchmarks/agent-durability/topology`](../../benchmarks/agent-durability/topology/README.md):
strict schema and schedule tests, append-only writer corruption tests,
independent-oracle negative controls, paired-runner contamination tests, and the
real process/service path. The tests use isolated temporary evidence roots; no
synthetic fixture is labeled or retained as pilot/publication evidence.

## Responsibility boundary

The shared harness supplies deterministic schedule construction, stable
identity validation, exact barriers, evidence confinement and sealing, and
independent admission. Temporal will supply actual parent, child, Activity,
history, replay, and recovery semantics only in the dependent case work. The
application and destination still own fixed membership, authority, retry
budgets, idempotency/version checks, and effect receipts.

## Falsifier

This finding is false if a listed corruption is admitted, the two arms can run
different work or policy without invalidating the pair, a first-arm outcome can
filter or stop the second arm, the frozen schedule can change undetected, or the
real process barrier cannot be tied to its exact process identity. Any topology
parity or cost claim made from this foundation alone would also exceed the
evidence.
