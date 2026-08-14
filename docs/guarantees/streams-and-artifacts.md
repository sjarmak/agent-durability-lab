# Streams and large artifacts

What the Workflow accepted is durable. What a process buffered, what a consumer
acknowledged, and what a blob store holds are three separate problems.

Back to the [guarantee summary](../guarantees.md).

## Agent event delivery across Continue-As-New

- **Temporal:** WorkflowStream can carry its log and cursor state to the next
  run, and Temporal retains the run chain.
- **Your application:** must pass stream state and stable session ownership into
  Continue-As-New and keep the run open long enough to drain terminal items.
- **Your destination:** consumer polling and retry, and any retention after
  Workflow close, remain application and transport concerns.
- **Evidence:** [one controlled continuation](../findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md)
  preserved the owner capability, approval gate, offsets, and ordered lifecycle
  events. Consumer acknowledgement and closed-run retention are unproven.

## Partial Workflow Stream output across Activity retry

- **Temporal:** accepted publish Signals and ordered offsets remain in Event
  History. A retried Activity uses a fresh publisher identity and does not
  recover the old process buffer.
- **Your application:** use the candidate SDK logical-output publisher and
  reconstructor, or equivalently mint a stable logical ID, fence generations,
  reset incremental rendering at begin, verify the content hash, and bind
  acknowledgement to the successful Activity terminal receipt and exact offset.
- **Your destination:** any external UI or broker still owns cursor durability,
  reconnect, retention, and delivery deduplication. The current OpenAI adapter
  does not return the terminal receipt needed for Workflow-side successful-attempt
  acknowledgement validation.
- **Evidence:** [Finding 0023](../findings/0023-workflow-stream-retries-need-output-reconstruction.md).
  The original nine trials distinguished pre-flush and post-flush behavior. The
  follow-on 36-trial raw, manual, and product run set replayed all histories:
  raw duplicated six outputs and accepted three stale acknowledgements, both
  protected arms reconstructed all outputs and rejected all six stale
  acknowledgements, and product matched manual's 18 stream batches.

## Large artifact publication and consumption

- **Temporal:** records durable procedure, Activity results, compact references,
  and retry. Experimental External Storage can offload large payload bytes but
  does not create an application acknowledgement.
- **Your application:** content-addressed blob, immutable logical reference,
  stable consumer acknowledgement, conflict validation, and explicit pending and
  orphan reconciliation.
- **Your destination:** storage must durably and conditionally publish each
  record, retain reachable content, and define collection and retention
  semantics.
- **Evidence:** [Finding 0024](../findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md).
  All 18 protected trials converged. The 3/3 unsafe reference, acknowledgement,
  and SDK-offload controls duplicated. Blob and pending-reference controls
  required explicit orphan reconciliation. All 36 histories replayed.
