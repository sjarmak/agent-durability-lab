# Sandbox Orchestration Harness recovery boundaries

This experiment tests the Temporal community Sandbox Orchestration Harness at
provider-effect boundaries. It uses the real `SandboxWorkflow` from upstream
commit `e8a88540d9523a3d9070860913567670194bacc1` and a hermetic provider whose
state can be inspected independently of Temporal.

## Question

Does the harness keep sandbox creation, commands, snapshots, attached writers,
and cleanup correct when a provider effect succeeds but the Activity does not
report success?

## Invariant

One current-authority sandbox lineage owns each logical provider operation. The
accepted provider status, command result, snapshot/fork lineage, and cleanup
disposition must agree with the independently observed provider state.

## Failure boundaries

The provider commits an effect, records its physical Activity attempt, and
blocks at an exact named barrier before returning. The controller verifies the
committed state and then releases the provider to return an injected retryable
error. This produces effect-success/Activity-failure ambiguity without a timing
guess.

The experiment also closes a parent while initialization is blocked after
provider creation, and submits commands through concurrent attached references.

## Oracle

The provider journal records the stable Workflow/Run/Activity operation key,
Temporal attempt, Worker/process identity, provider instance or snapshot,
workspace hash, authority generation, cleanup disposition, and UTC time. The
common append-only writer and independent oracle judge duplicate effects and
stale-authority acceptance. Sandbox-specific assertions independently compare
provider resources, snapshot lineage, and cleanup truth.

## Arms

- `unsafe`: each Activity delivery applies the provider effect again.
- `idempotent`: the provider atomically stores the stable inner Activity
  operation key with its receipt and returns that receipt on retry.
- `fenced`: idempotency plus generation/capability validation for attached
  command writers.

## Responsibility split

Temporal durably records the child Workflow, Updates, retry decisions, and
completed Activity results. The harness supplies lifecycle ordering and stable
outer Update IDs. The provider adapter supplies inner operation identity,
idempotency, resource lookup, workspace lineage, and cleanup receipts. A sandbox
reference supplies routing identity only; it is not a revocable owner
capability.

## Falsifier

The bounded protected claim is false if a logical provider operation is applied
twice, a stale attached writer mutates the workspace, a restored instance does
not match its declared snapshot prefix, cleanup reports success while a live
unowned instance remains, the named fault is not bracketed by durable provider
observations, or source/version provenance is incomplete.

## Run commands

```bash
go test -race ./...
go test -race -coverprofile=coverage.out ./...
go run ./cmd/sandbox-harness-evidence \
  --evidence-root evidence/sandbox-harness-YYYYMMDD-vN \
  --temporal-path "$(command -v temporal)" \
  --trials 3
```

## Observed result

The admitted v7 suite contains 36 live trials against the pinned upstream
`SandboxWorkflow`: 18 expected unsafe failures and 18 protected passes. The
four provider operations (`Start`, command, snapshot, and `Stop`) each ran three
unsafe and three idempotent trials. Each first attempt committed at the named
barrier; the controller read that commit before release; Temporal then retried
the same Activity operation. Unsafe retries applied twice. Idempotent retries
returned the stored receipt without reapplying.

Snapshot trials also forked a second upstream Workflow. Every fork restored the
exact pre-snapshot effect prefix, while the origin continued independently.
Unsafe snapshot retries created an extra unreferenced snapshot; protected
retries returned one stable snapshot receipt.

All three unsafe attached-reference trials accepted a generation-1 command
after generation 2 became current. All three fenced trials recorded and
rejected the stale write. The opaque harness reference routed both requests but
did not itself carry revocable authority.

In all three unsafe parent-close trials, provider creation committed before the
child's cancellation request and the harness left the unaccepted instance
active because no provider status had reached Workflow state. In all three
protected trials, a separate provider reconciler found the instance by stable
session identity and recorded a stop receipt. Retry idempotency alone did not
perform that cleanup.

The current parent and pinned child Workflow definitions replay the captured v7
parent-close histories without nondeterminism.

Evidence: [`sandbox-harness-20260808-v7`](evidence/sandbox-harness-20260808-v7).
V1 is a successful provider-operation subset. V2 and v3 are preserved failed
harness/verifier runs with explicit `failure.json` records. V4 produced the same
36 verdicts but used a semantic adapter label; v5 binds both adapter and agent
identity to the exact executable SHA-256. V6 additionally enforces monotonic
authority updates and includes the executable hash in parent-close manifests.
V7 reruns the same matrix from the reproducibly built, tidied module graph. Only
v7 contributes to the claim. The injected failure is a retryable error
after a proven provider commit, not a Worker `SIGKILL` or a claim about any
remote provider.
