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
