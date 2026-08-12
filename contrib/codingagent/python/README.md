# temporal-coding-agent

This is the incubating Python 3.12 binding for
[`specs/coding-agent-durability/v1`](../../../specs/coding-agent-durability/v1/README.md).
It is a small protocol kernel, not an agent framework. It has no Temporal or
OpenAI dependency and performs no provider, process, destination, or network IO.

The kernel keeps lifecycle separate from authority, preserves stable session,
turn, operation, and effect identities, fences executor operations by generation
and a SHA-256 capability digest, and returns immutable receipts. Every method
returns a new `ProtocolKernel`; the prior snapshot is unchanged.

```python
from temporal_coding_agent import (
    ExecutorAuthorization,
    LogicalIdentity,
    OperationRequest,
    OwnerCapability,
    ProtocolKernel,
)

request = OperationRequest(
    operation_id="operation:claim",
    request_hash="sha256:" + "2" * 64,
    transition_id="transition:claim",
    receipt_id="receipt:claim",
    occurred_at="2026-08-11T12:00:00Z",
)
capability = OwnerCapability.mint()
kernel = ProtocolKernel(LogicalIdentity("session:reviewer", "turn:42"))
kernel, claimed = kernel.claim(
    request,
    owner_capability=capability,
    coordinator_authenticated=True,  # Result of application-owned authentication.
)

# Raw capabilities are request-bound secrets. export_secret()/parse() exist
# solely for an application-owned secret store outside history and evidence.
authorization = ExecutorAuthorization(generation=1, capability=capability)
```

`claim`, `replace`, `cancel`, `mark_unresolved`, `record_stop_delivery`, and
`acknowledge_stop` require an independently authenticated coordinator. The
remaining seven operations require the current generation and capability.
Calling the same operation ID with the same request hash returns the original
receipt without another transition. Changed content raises
`OperationConflictError`; stale, revoked, and canceled callers receive the
corresponding typed rejection error.

An exact replay reads the immutable original receipt and is authorized against
the capability that committed it, including after replacement or terminal
revocation. That historical owner still cannot create a new operation.
Coordinator replays require application authentication. `request_hash` is the
caller's canonical content hash; claim/replace authority inputs and typed
operation subjects are also bound. Transition timestamps and receipt allocation
metadata describe the delivery envelope, not new operation content.

When no registration receipt already names the executor, `attach` also requires
`executor_discovered=True`. Set it only after an application-owned supervisor
or provider lookup authenticates the exact identity.

## Strict schema adapter

`loads_strict` rejects malformed UTF-8, duplicate object keys, and non-finite
JSON numbers. `SchemaCorpus(schema_dir)` integrates the shared Draft 2020-12
schemas and adds binding checks for actual UTC instants, request-hash equality,
generation preservation, exact replacement increments, evidence identity
agreement, per-source monotonic sequences, and confined artifact/history paths.
It caps documents at 4 MiB, 64 nesting levels, and 10,000 entries per
collection. Applications should confine and allowlist a schema directory before
constructing the adapter.

## Development

From this directory:

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy src tests
```

The tests include isolated model/kernel cases, integration against every shared
valid and invalid fixture, and the critical claim-to-effect-to-completion flow
with an exact replay.
