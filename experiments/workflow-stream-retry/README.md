# Workflow Stream retry reconstruction

## Status

Observed on one Linux `x86_64` host with Python `3.12.3`, Temporal Python SDK
`1.31.0`, Temporal CLI `1.8.0`, and Server `1.31.2`. Workflow Streams are a
Public Preview API and the Python module still identifies itself as experimental.
This is a bounded mechanism result, not a compatibility or performance claim.

## Question and invariant

What does a Workflow Stream consumer observe when an Activity publisher's Worker dies
before or after `flush()`?

One logical output is reconstructed only when the application distinguishes
process-local buffering from server-admitted stream items and carries explicit
Activity-attempt and publisher identity. An Activity retry is not assumed to continue the
old publisher or output prefix.

## Failure boundary

An authenticated, single-use Unix-socket barrier stops attempt 1 at one of two exact
points:

- `pre-flush-loss`: `A` and `B` are buffered in the client, then the Worker receives
  `SIGKILL` before `flush()`.
- `post-flush-duplicate`: attempt 1 publishes and awaits admission of `A` and `B`, then
  the Worker receives `SIGKILL` before Activity completion.

Temporal records the heartbeat timeout and starts attempt 2 on a replacement Worker.
Attempt 2 uses a fresh stream publisher, emits an explicit retry marker, publishes
`ABC`, flushes, and completes. The Workflow remains open until the consumer acknowledges
the terminal offset. An unfaulted arm is the positive control.

## Oracle

The independent disk auditor rejects anything other than the exact three-scenario,
three-trial schedule and recomputes the result from raw Event History. It binds Workflow
input/result, Activity result and heartbeat retry cause, stream signal actor, publisher
ID/sequence, event offsets, process identities and exit codes, barrier receipt, UTC event
bracketing, source/runtime provenance, exact inventory and hashes, and replay.

The distinguishing negative control concatenates every chunk without interpreting the
retry marker. It must produce `ABABC` in every post-flush trial. The retry-aware
reconstructor must reset at the marker and produce `ABC` in all nine trials.

## Run

From this directory:

```bash
uv sync --locked
uv run pytest --cov=workflow_stream_retry --cov-branch
uv run python -m workflow_stream_retry.audit \
  evidence/workflow-stream-retry-20260812-v4
```

Fresh evidence requires an absent, confined relative output directory:

```bash
uv run python -m workflow_stream_retry.run_experiment \
  --output evidence/workflow-stream-retry-YYYYMMDD-vN
```

## Evidence and observed result

The admitted population is
[`workflow-stream-retry-20260812-v4`](evidence/workflow-stream-retry-20260812-v4).
Its manifest SHA-256 is
`29338552fb91bd350427116566a8a7d56b96201b3b14ca0c8e5816b982a4329a`;
the report SHA-256 is
`8e2db6ca0cb859a4a86498ee556bcba75948343422e684eeea0252d481bfaaa2`.
The exact 20-file inventory contains nine replayed Event Histories.

- Three unfaulted trials exposed one publisher batch and `ABC`.
- Three pre-flush trials exposed no attempt-1 items; attempt 2 exposed retry + `ABC`.
- Three post-flush trials exposed two fresh publisher identities. Naive reconstruction
  produced `ABABC`; retry-aware reconstruction produced `ABC`.

The completed v1-v3 populations remain preserved but are superseded. Review after each
capture found progressively stricter audit-boundary gaps: scalar type and exact retry
binding in v1, cancellation cleanup after v2, and evidence-root/subprocess provenance
isolation after v3. V4 was captured only after those validators and cleanup paths passed
their adversarial controls.

## Responsibility split

- Temporal stores accepted stream publish Signals, offsets, Activity retry state, Workflow
  decisions, and results in Event History.
- The Workflow owns the logical output identity, waits for a terminal consumer
  acknowledgement, and returns only after the publishing Activity completes.
- The Activity publisher owns explicit retry markers and awaits `flush()` before calling a
  prefix admitted.
- The consumer reconstructs logical output across attempt/publisher generations. An
  external UI or message transport still needs its own durable cursor and delivery policy.

## Limits and falsifier

This does not establish closed-Workflow retention, reconnect behavior, Continue-As-New
composition, cross-host delivery, provider token semantics, arbitrary chunking,
performance, or exactly-once UI delivery. It uses no model/provider call.

The conclusion is falsified if a pre-flush item appears in history, an admitted post-flush
prefix disappears, offsets reorder, a retry reuses the old publisher identity, the naive
post-flush control stops producing exactly `ABABC`, retry-aware reconstruction differs
from `ABC`, the terminal result disagrees with the acknowledged stream, or any captured
history fails replay.

## SDK product candidate

The follow-on product experiment compares three arms under unfaulted,
pre-flush `SIGKILL`, post-prefix-flush `SIGKILL`, and
terminal-flushed-before-ack `SIGKILL` boundaries:

- `raw`: the unmodified Workflow Streams publication/subscription contract;
- `manual`: an expert application-owned generation/hash/ack protocol; and
- `product`: the proposed generic SDK publisher, incremental reconstructor, and
  Workflow-side exact terminal acknowledgement validator.

Each arm/scenario pair runs three times. The live runner preserves exact Worker
process identities, barrier receipts, consumer observations, Signal batch actors,
Activity retry causes, terminal receipts, acknowledgement outcomes, Event History,
history event/JSON sizes, replay, source/runtime pins, and a strict manifest. It
also measures application-owned protocol lines, state fields, and branches under
an explicit AST-counting definition.

The current candidate patch and reproduction instructions are in
[`../../contrib/sdk-python-retry-aware-streams`](../../contrib/sdk-python-retry-aware-streams).
Run the product population with the pinned Python 3.12 environment and patched
SDK source on `PYTHONPATH`:

```bash
PYTHONPATH=/path/to/sdk-python:$PWD .venv/bin/python \
  -m workflow_stream_retry.run_product_experiment \
  --output evidence/workflow-stream-product-YYYYMMDD-vN \
  --sdk-python-root /path/to/sdk-python
```

Superseded product roots are retained append-only with their correction reasons.
Only the root named in Finding 0023 is admitted. This comparison makes no
latency, throughput, external-delivery, provider-effect, or exactly-once claim.

The admitted candidate population is
[`workflow-stream-product-20260812-v4`](evidence/workflow-stream-product-20260812-v4).
Its strict 74-file inventory contains 36 independently audited and replayed
histories. Manifest SHA-256:
`1c5248343f3216c2113337cafe3cf43566a2e7698c88a5b0b5c03c29766aff18`;
report SHA-256:
`b4d9db78c9625478e33e924804aaeb6a038ba5e47a3602a3c30499b330bfc6c9`.

Observed in that population:

- raw reconstruction duplicated output in all six post-flush and
  terminal-before-ack trials, and accepted the stale attempt-1 terminal in all
  three terminal-before-ack trials;
- manual and product reconstructed `ABC` in all 24 protected trials and each
  rejected all three stale attempt-1 terminal acknowledgements;
- product used 18 stream Signal batches, exactly matching manual's 18. It added
  no history events and 3,805 JSON bytes across 12 trials versus manual;
- the registered application-owned recovery surface fell from 243 nonblank
  protocol lines, 12 state fields, and 18 AST branches in manual to three SDK
  operations and no application-owned protocol implementation in product; and
- combined exact-population and focused branch-aware coverage was 89% for the
  relevant product mechanism.

Product evidence roots v1-v3 remain append-only and superseded: v1 misnamed the
actual CLI used by the ephemeral service, v2 used Python 3.11 despite the
experiment's Python 3.12 contract, and v3 preceded the final diff/schedule/
acknowledgement/timestamp audit binding. None is used in the admitted claim.
