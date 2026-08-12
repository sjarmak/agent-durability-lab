# Retry-aware agent output for the Temporal Python SDK

## Status

This is an upstream contribution artifact against
`temporalio/sdk-python@d489a5dd679094f6580556dc531c9f1e1515b804` with SDK Core
`999e5a7dc8bbb8c457322ccb8e1806a0e780be95`. It extends the existing
experimental Workflow Streams and OpenAI Agents integration; it is not another
agent framework or a released Temporal API.

## Product improvement

An Activity retry can create a new stream publisher after an earlier publisher
durably emitted a partial model response. Raw subscribers then see both
prefixes. The patch adds:

- a provider-neutral `LogicalOutputEvent` envelope with stable logical output
  identity, monotonic generation, publisher and Activity-attempt identity,
  per-generation sequence, typed payload, and terminal content hash;
- `LogicalOutputPublisher` and `LogicalOutputReconstructor` helpers;
- validated begin/chunk/terminal updates for incremental rendering and explicit
  replacement-generation reset;
- Workflow-side validation that binds an acknowledgement to the successful
  Activity receipt and the exact terminal stream offset; and
- opt-in `ModelActivityParameters(streaming_retry_aware=True)` support in the
  existing OpenAI Agents integration. Raw streaming remains the default.

The patch is in
[`sdk-python-d489a5d-retry-aware-streams.patch`](sdk-python-d489a5d-retry-aware-streams.patch).
Its SHA-256 is
`2d72c81965753074cd1d8305c2ab9767e1bd1a1811ea7ee2f43fdd1d3bcb0684`.

The OpenAI Agents opt-in uses the envelope and reconstructor, but its existing
streaming model interface returns only model events. It does not expose the
successful Activity terminal receipt to Workflow code. Applications requiring
successful-attempt acknowledgement validation must use the generic publisher
from an Activity that returns its `LogicalOutputTerminal` receipt.

## Invariant, boundary, and oracle

Invariant: after a replacement Activity attempt begins, an older generation
cannot contribute to the reconstructed logical output or satisfy the successful
attempt's terminal acknowledgement.

Failure boundaries: Worker loss before flush, after a prefix flush, after a
terminal flush but before Activity completion, and after the successful
terminal but before consumer acknowledgement.

Oracle: compare the unmodified raw stream, the existing manual reset reference,
and this product helper under the same exact fault schedule. The raw post-flush
control must duplicate the prefix; the product helper must reconstruct only the
winning generation, reject stale and malformed envelopes, validate the exact
terminal offset/hash, and replay every captured history.

Falsifier: the product accepts an old or competing generation, a sequence gap,
a mismatched terminal count/hash, or an acknowledgement for a different
terminal or offset; changes raw-mode behavior; requires provider-specific
consumer recovery code; or exceeds the registered one-extra-batch-per-retry
bound relative to the manual reference.

## Apply and verify

```bash
git clone https://github.com/temporalio/sdk-python.git
cd sdk-python
git checkout d489a5dd679094f6580556dc531c9f1e1515b804
git submodule update --init --recursive
git apply --check /path/to/sdk-python-d489a5d-retry-aware-streams.patch
git apply /path/to/sdk-python-d489a5d-retry-aware-streams.patch
uv sync --all-extras
uv pip install protoc-wheel-0
uv run --with poethepoet poe build-develop
uv run --with poethepoet poe lint
uv run pytest tests/contrib/workflow_streams +  tests/contrib/openai_agents/test_openai_streaming.py
```

The controlled Worker-loss experiment and admitted measurements live beside
the lab experiment rather than in this contribution directory. This artifact
does not claim exactly-once delivery or workstation latency/throughput.
