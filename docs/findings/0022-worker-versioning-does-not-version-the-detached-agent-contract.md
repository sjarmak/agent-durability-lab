# Finding 0022: Worker Versioning does not version the detached-agent contract

**Status:** observed in nine admitted real-service trials

**Versions:** Temporal CLI `1.8.0`; Server `1.31.2`; Go SDK `1.47.0`;
Go `1.25.12`; Linux amd64

## Claim

Temporal Worker Deployment Versioning controlled where Workflow and Activity
tasks ran, but it did not decide whether a newer Activity implementation could
safely attach to a detached agent created by an older build. That compatibility
decision remained an application protocol.

In three auto-upgrade trials, Workflow history moved from `worker-v1` to
`worker-v2`; phase two ran on `worker-v2` and attached to the durable
`agent-v1` session because the new implementation explicitly admitted that
build. In three pinned trials, both phases stayed on `worker-v1` after
`worker-v2` became current. In three incompatible trials, history moved from
`worker-v1` to `worker-v3`, but the `worker-v3` Activity rejected stored
`agent-v1` before changing the registry.

Workflow replay compatibility was independent of both routing and agent
compatibility. The current Workflow replayed all nine histories. A deliberately
changed Workflow that inserted a new Timer command before the first Activity
was rejected by Temporal replay.

## Evidence and admission

The admitted
[`worker-versioning-20260812-v9`](../../experiments/worker-versioning-compat/evidence/worker-versioning-20260812-v9)
root contains the nine histories, Activity/Workflow identities, exported and
durable registry records, exact manifest, report, and executable provenance.
The manifest SHA-256 is
`e710195cd9602e0c91e6c017688c39dfa89f18b5132448a01e3c233fc3e9fc01`.
The disk auditor reconstructs deployment builds from raw Workflow-task events,
replays every history, compares the BoltDB and exported registry views, verifies
the exact inventory and hashes, and reruns the incompatible replay control.

V1-v8 remain preserved correction lineage. V1 observed the Workflow build
before the signal task boundary; v2 fixed that placement; v3 added executable
provenance; v4 added the required repetitions but preceded independent receipt
decoding from Event History. V5 closed that gap but preceded exact inventory,
strict durable-registry JSON, and root-bound Workflow identity checks. V6
added those checks; v7 preceded final terminal-event, registry/history, and
path bindings. V8 added those bindings but did not bind the explicit
incompatible-rejection verdict or fully validate runtime provenance. V9 closes
those gaps. None of the older roots is counted in the admitted result.

## Responsibility split

- Temporal supplies durable routing under pinned or auto-upgrade behavior and
  replayable Workflow history.
- Workflow code must evolve without introducing nondeterministic commands.
- The Activity implementation declares the detached-agent builds it understands
  and records its Worker identity in the receipt.
- The durable application registry binds stable session identity to the agent
  build and atomically rejects incompatible attachment.

## Limits and falsifier

This is a single-host local-service mechanism experiment with a simulated
detached agent and BoltDB. It does not establish provider compatibility,
cross-host attachment, rollback safety, schema migration, supervisor/store
recovery, or exactly-once effects.

The claim is falsified if a pinned run moves to the new deployment, an
auto-upgrade run cannot account for both Worker builds, an incompatible attach
changes durable state, a current compatible history fails replay, or the
deliberately incompatible Workflow replay succeeds.
