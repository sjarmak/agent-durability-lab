# Worker Versioning and detached-agent compatibility

## Status

Observed on one Linux amd64 host with Temporal CLI `1.8.0`, Server `1.31.2`,
Go SDK `1.47.0`, and Go `1.25.12`. The admitted v9 population contains nine
real-service runs: three each for auto-upgrade compatibility, pinned
compatibility, and auto-upgrade incompatibility.

## Question

When a long-lived logical agent session crosses a Worker Deployment change,
which compatibility contract comes from Workflow replay, which comes from the
Activity implementation, and which must be enforced by the detached agent's
durable registry?

## Invariant

A deployment change may route later Workflow and Activity tasks to a new Worker
build, but it must not silently reinterpret an existing detached-agent build.
A compatible Worker attaches to the exact stored session and records both
identities. An incompatible Worker fails before changing the registry. Workflow
code must separately replay the complete history.

## Failure boundary and controls

Each run starts phase one on `worker-v1`, which creates `session-1` with
`agent-v1`. The controller then starts a second deployment version, makes it
current, and signals phase two.

- `auto-compatible`: `worker-v2` accepts `agent-v1` and attaches.
- `pinned-compatible`: the Workflow and Activity stay on `worker-v1`, even
  after `worker-v2` becomes current.
- `auto-incompatible`: `worker-v3` accepts only `agent-v3`; it rejects the
  stored `agent-v1` without an attachment.

The replay negative control inserts a Timer command before the first recorded
Activity. Temporal rejects that deliberately incompatible Workflow against the
captured history.

## Oracle

The independent disk audit fails closed unless all of these agree:

- the exact nine-run schedule and exact file inventory;
- SHA-256 and byte length for every raw artifact;
- Workflow-task deployment versions reconstructed from Event History;
- Activity receipts recording Worker build, detached-agent build, Temporal
  attempt, Workflow/Run identity, and phase;
- the exported registry and the durable BoltDB registry;
- compatible replay of all nine histories;
- rejection of the deliberately incompatible Workflow replay; and
- zero registry mutation in every incompatible trial.

## Run

```bash
go test -race -count=1 -timeout=3m \
  ./experiments/worker-versioning-compat/internal/lab \
  -run '^TestLiveWorkerVersioningSessionCompatibility$'

go run ./experiments/worker-versioning-compat/cmd/experiment \
  --output experiments/worker-versioning-compat/evidence/<new-unique-root>

go run ./experiments/worker-versioning-compat/cmd/evidence-audit \
  experiments/worker-versioning-compat/evidence/worker-versioning-20260812-v9
```

Every experiment output directory is created exclusively. Use a new root; do
not rewrite an existing population.

## Evidence and observed result

The admitted root is
[`worker-versioning-20260812-v9`](evidence/worker-versioning-20260812-v9).
Its manifest SHA-256 is
`e710195cd9602e0c91e6c017688c39dfa89f18b5132448a01e3c233fc3e9fc01`.
The root contains 38 regular files and 489,286 bytes. Across three repetitions
per scenario:

- auto-upgrade histories and Workflow observations moved from `worker-v1` to
  `worker-v2`; phase two attached `worker-v2` to stored `agent-v1`;
- pinned histories stayed on `worker-v1`; phase two attached from `worker-v1`;
- incompatible histories moved from `worker-v1` to `worker-v3`, but all three
  phase-two requests failed nonretryably and the durable registries retained
  `agent-v1` with zero attachments;
- all nine histories replayed with the current Workflow; and
- the incompatible Workflow replay was rejected.

V1 is retained because its Workflow-side build observation occurred before the
signal task boundary. V2 corrected that observation but preceded executable
provenance. V3 added executable provenance but had one trial per scenario. They
remain append-only correction lineage. V4 added three trials per scenario but
its auditor did not independently decode Activity receipts from Event History;
v5 closed that gap but preceded exact inventory, strict durable-registry JSON,
and root-bound Workflow identity checks. V6 added those checks. V7 preceded
final terminal-event, registry/history, and path bindings. V8 added those
bindings but its auditor did not bind each scenario's explicit rejection flag
or fully validate OS, architecture, and executable-digest provenance. V9 adds
those checks and derives run identity from the output root before service work.
V1-v8 are not the admitted population.

## Responsibility split

- Temporal routes Workflow and Activity tasks according to Worker Deployment
  versioning and retains the replayable history. Auto-upgrade and pinned routing
  do not validate an external agent protocol.
- Workflow authors keep command evolution replay-compatible and choose pinned
  versus auto-upgrade behavior deliberately.
- Activity code records its own Worker build and declares which detached-agent
  builds it can safely attach to.
- The application registry atomically binds the stable session to the original
  agent build and rejects incompatible attach attempts before mutation.

## Limits and falsifier

This is a single-host local-service experiment with a BoltDB registry and a
simulated detached-agent identity. It does not test a real model provider,
cross-host process discovery, mixed SDK versions, rolling database migration,
Worker Deployment rollback, supervisor/store loss, or performance. It is not
an exactly-once or general provider-compatibility claim.

The finding is falsified if an auto-upgrade history or receipt lacks the new
Worker identity, a pinned run executes phase two on the new Worker, an
incompatible Worker attaches or changes the registry, a compatible history no
longer replays, or the incompatible Workflow replay succeeds.
