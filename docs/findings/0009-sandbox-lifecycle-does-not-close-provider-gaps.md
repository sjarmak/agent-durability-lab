# Finding 0009: Durable sandbox lifecycle does not close provider gaps

**Status:** observed in 36 admitted live trials against the pinned community
Sandbox Orchestration Harness

**Versions:** Sandbox Orchestration Harness
`e8a88540d9523a3d9070860913567670194bacc1`; Temporal Server `1.31.2`;
Temporal CLI `1.8.0`; Go SDK `1.47.0`; Go `1.26.2`; Linux amd64

**Source identity:** experiment executable and evidence adapter
`b775a6142770467158fe6f61b3c16c183ae754731dc551e9ead8cf6f7ea55402`

## Claim

The harness gives a sandbox a durable child Workflow, lifecycle state, stable
outer Update IDs, explicit cleanup paths, attachable routing references, and
snapshot/fork operations. Those are useful orchestration primitives. They do
not make the provider calls inside retryable Activities idempotent, turn an
opaque attached reference into revocable authority, or reconcile a provider
resource whose identity never reached Workflow state.

At exact post-provider-effect barriers for `Start`, command, snapshot, and
`Stop`, all 12 unsafe trials applied the same inner Activity operation twice.
All 12 protected trials also recorded two physical Activity deliveries, but a
provider transaction keyed by stable Workflow/Run/Activity identity returned
the first receipt on retry and applied once. The common oracle scored the
unsafe runs `valid-fail / duplicate_physical_effect` and every protected run
`valid-pass`.

Stable Update identity and stable provider-operation identity solve different
delivery boundaries. A full-Workflow integration test submitted the same outer
Update ID twice: the Update handler ran once, while its one inner command
Activity still retried twice after the first provider commit. Update
deduplication therefore cannot substitute for provider effect idempotency.

## Attached writers and cleanup

Two distinct attached references concurrently submitted commands after
generation 2 became current. In all three unsafe trials, the generation-1
command changed the workspace; in all three fenced trials, the provider
recorded and rejected it before the effect barrier. The common stale-generation
oracle produced three expected failures and three passes. The harness reference
provided addressability, not ownership authority.

The parent-close boundary exposed a separate gap. Provider creation committed,
then the child durably received cancellation before initialization completed.
In all three unsafe trials, the harness had no accepted provider status and left
one active unowned instance. Three protected trials passed only after an
external provider reconciler located the instance by stable session identity
and recorded a stop receipt. Provider retry idempotency was necessary for
ambiguous creation but was not sufficient for orphan cleanup.

Parent and child terminal status was not a reliable cleanup oracle. On the
tested upstream path, the durable child cancellation request could coexist with
normal parent completion. Provider state and cleanup receipts were the
independent truth.

## Snapshot result

Each of the six snapshot ambiguity trials created a snapshot after one command,
then applied a second command to the origin and initialized another pinned
`SandboxWorkflow` from the accepted snapshot. Every fork contained exactly the
first command and declared the accepted snapshot as its parent. Unsafe retries
also created an unreferenced extra snapshot; protected retries returned the
stored snapshot receipt. Snapshot lineage preserved the tested workspace
prefix, not a vendor transcript or arbitrary external effects.

## Responsibility split

- Temporal durably records Workflow state, Update admission/deduplication,
  Activity retries, child cancellation, and completed Activity results.
- The harness orders sandbox lifecycle operations and stores accepted provider
  status and snapshot references.
- The provider adapter must supply a stable operation key, atomic receipt/effect
  storage, resource lookup, authority fencing, snapshot semantics, and cleanup
  receipts.
- A reconciliation loop must resolve resources created before their identity is
  accepted into Workflow state. Neither Activity retry nor disconnected cleanup
  can name an unknown resource.
- The vendor agent layer still owns transcript, turn, tool-effect, approval, and
  result durability. None follows from sandbox lifecycle durability.

## Limits

The provider is hermetic bbolt state, not E2B, Daytona, Modal, GKE, AgentCore,
or another remote backend. The fault is an injected retryable error after a
proven provider commit, not a killed Worker or lost network response. Worker
attempts ran in one local process. Three trials per arm show repeatability, not
a failure rate. `Stop` duplication was observable in the provider journal but
did not resurrect an already stopped resource. Snapshot content was a
deterministic effect list, not a real filesystem image. No Claude Code or Codex
process or vendor transcript was involved. The generation capability values are
explicit non-secret fixtures and appear in Temporal history; a production
bearer capability would require an encrypted Payload Codec or an opaque trusted
reference rather than plaintext Workflow input.

## Evidence and falsifier

The admitted evidence is
[`sandbox-harness-20260808-v7`](../../experiments/durable-vendor-sessions/sandbox-harness/evidence/sandbox-harness-20260808-v7).
It contains 36 run directories, full Temporal histories, provider journals,
named barriers, source/version settings, cleanup receipts, and independent
verdicts: 18 expected `valid-fail` controls and 18 `valid-pass` protected arms.
The current parent and pinned child Workflow registrations also replay the
captured protected parent-close histories without nondeterminism.

V1 is a successful 24-run provider-operation subset. V2 preserves a runner
barrier-release bug; v3 preserves an overstrict parent-status verifier. Their
partial results are not admitted. V4 produced the same 36 verdicts but its
adapter-version field was a semantic label; v5 pins that field and the agent
identity to the exact executable hash. V6 adds monotonic authority updates and
pins parent-close manifests to the same executable. No failing raw evidence was
deleted or rewritten. V7 is the source-identical rerun after `go mod tidy`; two
independent builds produced its recorded executable SHA-256.

The claim is falsified if a protected retry applies twice, an unsafe retry no
longer distinguishes the arms, a fenced stale reference mutates the workspace,
a fork differs from its declared snapshot prefix, protected reconciliation
leaves an active unowned instance or lacks a cleanup receipt, the fault is not
bracketed by provider observations and Temporal history, or a source-pinned
three-trial rerun changes the stated verdicts.
