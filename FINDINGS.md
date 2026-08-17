# Findings

Each line is one supported claim and the thing to do about it. Numbers in the
claims are trial counts from preserved runs, not projections. Full method,
evidence links, and scope limits live on each finding page.

Two rules hold everywhere in this list. A coordinator's delivery attempt is
not agent identity. One recorded coordinator completion is not one external
effect. Findings are grouped by mechanism first; the **Evidence** column names
which implementation(s) produced the numbers.

## Identity and ownership

| # | Claim | Do this | Evidence |
| --- | --- | --- | --- |
| [0001](docs/findings/0001-worker-death-surviving-agent.md) | After Worker `SIGKILL`, Temporal redelivered the Activity; whether attempt 2 created a competitor, reattached, or rejected the old writer was decided by the application protocol alone. | Decide attach, replace, or reject in durable application state before the retry runs. | Temporal only |
| [0002](docs/findings/0002-launch-decision-is-not-process-liveness.md) | A Worker that died between the durable launch decision and `exec` left attempt-2 code attached to a PID-less phantom in the control arm; fenced conditional replacement completed. | Do not infer a running process from a recorded launch. | Temporal only |
| [0003](docs/findings/0003-activity-id-completion-is-not-attempt-scoped.md) | Attempt 1's task token was rejected in 3/3 trials, but completion by Workflow/Run/Activity ID accepted an obsolete attempt's result in 3/3 unsafe trials; the fenced arm rejected it. | Authorize a current owner before calling `CompleteActivityByID`. | Temporal only |
| [0005](docs/findings/0005-launch-pending-does-not-identify-process-reality.md) | One `launch_pending` record with no PID means either no child exists or a live child has not registered; Temporal history cannot separate them. | Prove liveness by exact process identity, or advance the fence before replacing. | Temporal only |
| [0013](docs/findings/0013-application-policy-equalizes-safety-not-recovery-cost.md) | Across 540 valid matched pairs, both unsafe systems accepted four obsolete actions after the owner label recurred and both fenced systems accepted zero; protected median recovery was 45.5 ms for Temporal and 1 ms for PostgreSQL on the pinned host. | The fence supplies safety, not the durability substrate; choose the substrate on operational fit. | Temporal + PostgreSQL |

## External agent runtimes (Claude Code, Codex): session identity is not effect authority

| # | Claim | Do this | Evidence |
| --- | --- | --- | --- |
| [0010](docs/findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md) | Putting `claude -p` in a retryable Activity duplicated the turn: 9/9 faulted trials launched two Claude sessions and applied two effects under one accepted outcome. | Do not treat a bare CLI call in an Activity as one execution. | Claude Code, Temporal-hosted |
| [0019](docs/findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md) | Caller-selected `--session-id` / `--resume` kept one Claude UUID across both attempts, and 9/9 faulted trials still applied two effects. | Use resume for transcript continuity; get safety somewhere else. | Claude Code, Temporal-hosted |
| [0020](docs/findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md) | A supervisor outside the Worker that owns generation and capability passed 15/15 protected runs at four boundaries with one process, effect, and workspace outcome each, while the matched resume-only controls duplicated 9/9. | Put start-or-attach and the fence in a supervisor the Worker does not own. | Claude Code, Temporal-hosted |
| [0021](docs/findings/0021-codex-thread-resume-is-not-turn-authority.md) | `codex exec resume` preserved one logical thread and still reproduced 6/6 post-effect duplicates; the fenced arm passed 27/27 across eight boundaries. | Same shape as Claude: thread identity is not turn authority. | Codex, Temporal-hosted |
| [0022](docs/findings/0022-worker-versioning-does-not-version-the-detached-agent-contract.md) | Worker Deployment Versioning routed tasks correctly, but the decision to attach a new build to an old detached agent stayed in application code: 6 compatible trials attached, 3 incompatible trials rejected without touching the registry. | Declare compatible agent builds in the Activity and reject atomically. | Claude Code, Temporal-hosted |

## External effects

| # | Claim | Do this | Evidence |
| --- | --- | --- | --- |
| [0004](docs/findings/0004-one-temporal-completion-can-hide-two-effects.md) | Temporal recorded one Activity completion while the application wrote the effect twice: 18/18 unsafe trials left two effects, 18/18 protected trials left one, across six destination classes. | Carry a stable effect ID and pick a protocol per destination; no single mechanism covered all six. | Temporal only |
| [0008](docs/findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md) | Replay restored completed model and tool Activity results inside a Temporal-native agent loop, and the unsafe tool still applied its effect twice in 3/3 trials. | Keep effect identity in the tool, not the loop. | Temporal only |
| [0009](docs/findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md) | Durable sandbox lifecycle did not make provider calls idempotent: 12/12 unsafe trials applied the inner operation twice, 12/12 receipt-keyed trials applied it once, and 3/3 unsafe attached references wrote after replacement. | Key provider operations by stable operation identity and reconcile resources the Workflow never named. | Temporal only |

## Cancellation

| # | Claim | Do this | Evidence |
| --- | --- | --- | --- |
| [0006](docs/findings/0006-cancellation-requires-application-revocation.md) | Workflow cancellation did not revoke a detached agent: 6/6 Temporal-only controls committed an effect after the Workflow closed as canceled, and 18/18 application-revoked runs accepted nothing. | Commit revocation in one work-store transaction before delivering any stop signal. | Temporal only |

## Streaming and large artifacts

| # | Claim | Do this | Evidence |
| --- | --- | --- | --- |
| [0023](docs/findings/0023-workflow-stream-retries-need-output-reconstruction.md) | A retried Activity publisher re-emitted its logical prefix: naive concatenation produced `ABABC` in every post-flush retry, while resetting at the retry marker reconstructed `ABC` in 9/9 trials. | Mint a logical output ID, mark the retry generation, and reset rendering at the marker. | Temporal only |
| [0024](docs/findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md) | Content, logical reference, and consumer acknowledgement need separate durable identities: 18/18 protected runs converged, and 3/3 unsafe runs each duplicated a reference, an acknowledgement, or a payload object. | Publish a content-addressed blob, an immutable reference, and an acknowledgement, and reconcile orphans explicitly. | Temporal only |

## Apparatus and calibration

These establish that the measuring instrument can fail, which is why the
results above are worth reading. They are not results about Temporal.

| # | Claim |
| --- | --- |
| [0007](docs/findings/0007-live-common-harness-calibrates-the-oracle.md) | The cross-system harness drove all four v1 boundaries and the oracle classified 12/12 unfaulted, 12/12 protected, and 12/12 unsafe runs correctly. |
| [0011](docs/findings/0011-aba-fencing-and-recovery-dynamics-apparatus-distinguish-controls.md) | The ABA and recovery-dynamics apparatus distinguished every unsafe control under contract `adl.cross-system.v2`. |
| [0014](docs/findings/0014-topology-foundation-fails-closed-before-pilot.md) | The topology foundation rejects contaminated schedules, lineage, barriers, paths, and replay before any pilot episode counts. |
| [0015](docs/findings/0015-topology-semantics-controls-distinguish-with-replay.md) | 44 canonical semantics runs: 26/26 unfaulted or protected passed, 18/18 unsafe distinguished, all histories replayed. |
| [0016](docs/findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md) | 52 canonical fan-out-32 recovery runs distinguished under bounded catch-up. A later executable change makes this root historical; a fresh run set is required before renewing a current-source claim. |
| [0018](docs/findings/0018-topology-measurement-admission-is-independent-before-pilot.md) | Admission reconstructs every registered metric from raw causal, dependency, destination, and native-history records instead of trusting self-reported fields. |

## Superseded

Kept because the corrections are part of the record.

| # | Claim | Superseded by |
| --- | --- | --- |
| [0012](docs/findings/0012-temporal-and-postgresql-pass-development-conformance-not-performance.md) | Both systems executed the same procedure under development conformance; no performance comparison was supported. | [0013](docs/findings/0013-application-policy-equalizes-safety-not-recovery-cost.md) |
| [0017](docs/findings/0017-topology-matrix-is-ready-for-pilot-not-publication.md) | The v5 topology apparatus was distinguishing but trusted several derived fields. | [0018](docs/findings/0018-topology-measurement-admission-is-independent-before-pilot.md) |

## Where to go next

- [Guarantee summary](docs/guarantees.md): who supplies each property, one line per cell.
- [`experiments/`](experiments/): contracts, harnesses, oracles, and raw run sets.
- [Experiment methodology](docs/experiment-methodology.md): what a run has to
  do before it is admitted here.
