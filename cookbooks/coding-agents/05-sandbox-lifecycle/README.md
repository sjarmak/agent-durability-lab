# Sandbox lifecycle

This cookbook is a thin executable guide to
[Finding 0009](../../../docs/findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md)
and the pinned
[Sandbox Orchestration Harness experiment](../../../experiments/durable-vendor-sessions/sandbox-harness/README.md).
It audits the admitted evidence in place and runs the experiment's focused contract and
replay tests. It does not copy the harness, regenerate evidence, or rewrite raw evidence.

## Question

Does a durable sandbox Workflow keep provider create, command, snapshot, stop, attached
writer, and parent-close behavior correct when a provider effect commits but its Activity
does not report success?

## Invariant

One current-authority sandbox lineage owns each logical provider operation. The accepted
provider status, command result, snapshot lineage, and cleanup disposition must agree with
the provider's independently observed state.

The sandbox ownership contract is separate from agent-session ownership. A sandbox may contain an agent
session, but a sandbox instance ID is not a transcript or turn identity, and an agent
session ID is not authority to mutate a sandbox. Keep both contracts explicit so resource
replacement cannot silently grant a stale agent writer access to the current workspace.

## Architecture slice

The upstream harness gives each sandbox a durable child Workflow, ordered lifecycle state,
an attachable routing reference, snapshot/fork operations, and a stable outer Update ID.
Those orchestration identities do not identify the provider effect delivered by a retryable
Activity. The adapter derives the inner operation key from stable Workflow ID, Run ID, and
Activity ID, while every delivery separately records Temporal attempt, Worker/process
identity, and UTC observation time.

In the protected path, one provider transaction atomically commits the effect and provider
receipt under that operation key. A retry with the same operation key returns the original
receipt without reapplying. In the unsafe path, the same stable outer Update ID can still
lead to two provider commits: Update deduplication and provider-operation idempotency close
different boundaries.

Attached references provide addressability, not ownership. The fenced provider stores a
monotonic owner generation and opaque capability hash, rejects a stale attached writer
before its effect, and allows only the current generation. Parent-close cleanup uses stable
session identity to discover resources whose provider status never reached Workflow state.

## Failure boundaries

For create (`Start`), command, snapshot, and stop, the provider commits its effect and
journal entry, then blocks at an exact named `provider-*-effect-committed` barrier. The
controller reads the committed provider state before releasing an injected retryable error,
causing the same Activity operation to be delivered again without a sleep-based window.

The attached-writer case advances provider authority to generation 2 before concurrently
submitting generation-1 and generation-2 commands. The parent-close case commits provider
creation, then durably requests child cancellation before initialization can return the
provider status. These boundaries distinguish effect ambiguity, stale authority, and an
unknown-resource cleanup gap rather than treating them as one failure mode.

## Oracle

The provider journal is independent of the Workflow outcome. For the four ambiguous
operations, the audit requires two physical deliveries of the same inner operation:

- each unsafe arm applies twice and is `valid-fail / duplicate_physical_effect`;
- each protected arm applies once, returns one stable provider receipt on retry, and is
  `valid-pass`.

The stale attached writer must be recorded with generation 1, rejected as
`stale_authority`, and absent from the workspace after generation 2 is current. Snapshot
verification follows the accepted parent snapshot ID and requires the fork's effects and
workspace SHA-256 to equal the declared workspace prefix exactly, even though the origin
continues independently. Parent-close verification reads provider state—not parent or child
terminal status—and requires the reconciler's stop receipt to match the final provider
journal with no active orphan.

The read-only `audit` mode seals the exact 368-file v7 tree and checks all 36 cited trial
directories, including provider state, stored verdicts, parent/child histories, and the
unsafe stale-writer negative control—not only manifest-listed common artifacts. The
`critical` mode runs the provider
contract, outer-Update/inner-receipt integration, snapshot, fencing, orphan reconciliation,
and captured history replay tests.

## Fresh-checkout run

Prerequisites are Python 3.12 and Go 1.26 or later. From the repository root, run:

```bash
./cookbooks/coding-agents/05-sandbox-lifecycle/run.sh all
```

The focused path uses the hermetic bbolt provider and Temporal's Go test environment. It
does not require a remote sandbox account, vendor credentials, or an already-running
Temporal service. Run the modes separately when only one gate is needed:

```bash
./cookbooks/coding-agents/05-sandbox-lifecycle/run.sh audit
./cookbooks/coding-agents/05-sandbox-lifecycle/run.sh critical
```

For the complete pinned experiment gate, including all package tests and history replay:

```bash
cd experiments/durable-vendor-sessions/sandbox-harness
go test -race ./...
```

The evidence-generation command in the experiment README requires a new evidence root.
Never point it at `sandbox-harness-20260808-v7`; evidence capture is append-only and this
cookbook never regenerates or edits the admitted suite.

## Evidence

The admitted raw suite is
[`sandbox-harness-20260808-v7`](../../../experiments/durable-vendor-sessions/sandbox-harness/evidence/sandbox-harness-20260808-v7).
It contains 36 source-pinned live trials: three unsafe and three protected trials for each
create, command, snapshot, stop, stale-writer, and parent-close boundary. Provider-operation
trials retain the named fault barrier, provider and destination state, combined provider
journal and Temporal history export, process observations, input hashes, common events,
and independent verdict. Parent-close trials retain separate parent and child histories,
provider state at close, final provider state, and the cleanup verdict.

The final suite pins upstream harness commit
`e8a88540d9523a3d9070860913567670194bacc1` and experiment executable SHA-256
`b775a6142770467158fe6f61b3c16c183ae754731dc551e9ead8cf6f7ea55402`.
Earlier runs are correction lineage and do not contribute to this cookbook's claim.

## Observed result

All 12 unsafe create/command/snapshot/stop trials applied the ambiguous provider operation
twice and failed with `duplicate_physical_effect`. All 12 protected trials recorded two
deliveries while atomically returning the first provider receipt on retry and applying once.
The full-Workflow integration also accepted one stable outer Update ID while its inner
command Activity had two physical provider deliveries, directly separating the two scopes.

All three unsafe attached-writer trials let generation 1 mutate after generation 2 became
current. All three fenced trials rejected the stale attached writer before mutation. All
six snapshot forks restored exactly `["snapshot-prefix"]` and the accepted snapshot's
workspace hash; unsafe retries additionally left an unreferenced snapshot, while protected
retries reused one snapshot receipt.

All three unsafe parent-close trials left one active provider orphan because no provider
status reached Workflow state. In every protected trial, a provider reconciler found the
instance by stable session identity, recorded a stop receipt, and left zero active
instances. The current registered parent and child Workflow definitions pass history replay
against the captured protected parent-close histories.

These observations apply to the hermetic provider and an injected retryable failure after a
proven commit. They do not establish behavior for a remote provider, Worker death, a vendor
agent transcript, or arbitrary external effects. Snapshot lineage preserves the declared
workspace prefix only; it does not preserve an agent transcript, credentials, or effects
outside that workspace.

## Responsibility split

- Temporal records Workflow and child state, Update admission/deduplication, Activity
  retries and results, cancellation requests, and replayable history.
- The harness orders sandbox lifecycle transitions and stores accepted provider status and
  snapshot references.
- Application code supplies stable resource and agent-session identities, chooses authority
  and cleanup policy, and invokes reconciliation after ambiguous parent close.
- The provider adapter atomically binds inner operation keys to effects and receipts,
  enforces generation fencing, supports lookup by stable session identity, defines snapshot
  semantics, and persists cleanup receipts.
- The vendor agent layer separately owns transcript, turn, approval, tool-effect, and result
  durability. Sandbox lifecycle durability does not supply those guarantees.

## Falsifier

The bounded conclusion is false if any protected retry reapplies its provider effect or
changes its receipt; an unsafe control stops distinguishing duplicate application; a stale
generation mutates the workspace; a fork differs from its declared workspace prefix or
accepted snapshot hash; protected parent-close reconciliation leaves an active orphan or
lacks the matching stop receipt; the provider journal or Temporal history no longer brackets
the exact fault; history replay becomes nondeterministic; or a fresh source-pinned
three-trial rerun changes the stated verdicts.
