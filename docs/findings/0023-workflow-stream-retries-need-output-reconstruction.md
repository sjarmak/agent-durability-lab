# Finding 0023: Workflow Stream retries need output reconstruction

A retried Activity publisher created a fresh publisher identity and re-emitted
its logical prefix. Naive concatenation produced `ABABC` on every post-flush
retry. Resetting at the retry marker reconstructed `ABC` in nine trials.

## Status

Observed and admitted for the pinned Python Public Preview surface on one Linux host.

## Claim

Workflow Streams durably expose items accepted by the Workflow-hosted stream, but they do
not by themselves define one logical model output across a retried Activity publisher.
Process-local items can disappear before `flush()`. Items admitted before Worker loss
remain visible, while the retried Activity creates a fresh publisher and can emit the
logical prefix again. Consumers therefore need an application-level logical output ID,
explicit retry/generation marker, and reconstruction rule.

## Observed boundaries

Across three trials per arm:

- killing attempt 1 before `flush()` lost its buffered `AB`; only attempt 2's retry marker
  and `ABC` appeared in Event History;
- killing attempt 1 after awaited `flush()` retained `AB`; attempt 2 used a different
  publisher identity and added retry + `ABC`;
- naive concatenation distinguished every post-flush retry as `ABABC`, while resetting at
  the retry marker reconstructed `ABC` in all nine trials; and
- the Workflow waited for acknowledgement of the exact terminal offset before closing.

Every faulted history recorded attempt 2 with attempt 1's heartbeat timeout as
`last_failure`. All nine histories replayed.

## Evidence and admission

The source-pinned
[`workflow-stream-retry-20260812-v4`](../../experiments/workflow-stream-retry/evidence/workflow-stream-retry-20260812-v4)
population has 20 exact files, nine histories, manifest SHA-256
`29338552fb91bd350427116566a8a7d56b96201b3b14ca0c8e5816b982a4329a`,
and report SHA-256
`8e2db6ca0cb859a4a86498ee556bcba75948343422e684eeea0252d481bfaaa2`.
The independent auditor reconstructs raw publish Signals, exact publisher/attempt/process
identity, results, retry cause, offsets, acknowledgement, provenance, and replay.

The follow-on SDK product population
[`workflow-stream-product-20260812-v4`](../../experiments/workflow-stream-retry/evidence/workflow-stream-product-20260812-v4)
contains 36 histories across raw, expert-manual, and SDK-product arms; four
boundaries; and three repetitions. Its 74-file manifest SHA-256 is
`1c5248343f3216c2113337cafe3cf43566a2e7698c88a5b0b5c03c29766aff18`,
and report SHA-256 is
`b4d9db78c9625478e33e924804aaeb6a038ba5e47a3602a3c30499b330bfc6c9`.
All 36 histories replayed and passed independent source/runtime/process/history
audit.

The raw arm duplicated logical output in six post-admission fault trials and
accepted all three stale attempt-1 terminal acknowledgements. Both protected
arms reconstructed `ABC` in all 24 trials and rejected all six stale
acknowledgement attempts. Product used 18 stream batches, the same as manual;
across 12 trials it recorded 333 versus 333 history events and 393,714 versus
389,909 history JSON bytes. Under the registered application-owned protocol
definition, the manual reference contains 243 nonblank lines, 12 state fields,
and 18 AST branches; product application code delegates those responsibilities
to three SDK operations and owns no corresponding protocol implementation.
Combined population and focused relevant coverage is 89%.

## Product pattern

Treat stream publisher identity as delivery identity, not logical output identity:

1. mint the logical output ID before the Activity;
2. include Activity attempt/generation in every item;
3. emit a structural retry/reset marker before replacement output;
4. await `flush()` before declaring a prefix admitted;
5. have the consumer durably acknowledge the exact terminal offset; and
6. keep the Workflow open until that acknowledgement or record a separate delivery
   disposition.

This belongs in SDK guidance and coding-agent cookbooks. A convenience API could expose a
logical-output generation/reset envelope, but should not hide the underlying publisher ID,
sequence, or acknowledgement boundary.

That convenience API is now implemented as an upstream candidate against pinned
`temporalio/sdk-python@d489a5d`: a provider-neutral logical output envelope,
publisher, incremental begin/chunk/terminal reconstructor, hashed terminal receipt,
and Workflow-side exact acknowledgement validator. The existing OpenAI Agents
integration has an opt-in retry-aware stream mode while preserving raw mode as the
default. The adapter reconstructs retries, but its current model Activity result does
not expose the successful terminal receipt; applications requiring Workflow-side
successful-attempt acknowledgement validation use the generic Activity API and return
that receipt explicitly.

## Responsibility split

Temporal supplies Event History, Workflow-hosted stream ordering, publisher-sequence
deduplication, Activity timeout/retry, and replay. The application supplies logical output
identity, reset semantics, exact completion acknowledgement, and any external delivery
cursor. A UI, broker, or other external destination supplies its own retention and
deduplication guarantees.

## Scope and what would change this conclusion

The result is limited to Python SDK `1.31.0`, CLI `1.8.0`, Server `1.31.2`, one host,
fixed `ABC` chunks, two exact kill boundaries, and a consumer active before Workflow
close. Workflow Streams remain Public Preview/experimental. No provider, Continue-As-New,
cross-host, performance, reconnect, closed-run retention, or exactly-once-delivery claim is
made.

The claim is falsified if a source-pinned repeat loses an admitted prefix, exposes a
pre-flush-only item, reorders offsets, silently merges fresh publishers, cannot reconstruct
one logical output from the declared reset contract, completes before its terminal
acknowledgement, or fails history replay.
