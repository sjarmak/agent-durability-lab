# Coding-agent durability protocol v1

This directory is the language-neutral contract shared by SDK bindings,
cookbooks, and fault-conformance profiles. It describes the application
protocol needed around Temporal; it does not claim that Activity retries make
external effects exactly once.

The controlling product scope and evidence matrix live in
[`docs/product/coding-agent-durability-v1.md`](../../../docs/product/coding-agent-durability-v1.md).
The admitted guarantees remain in [`docs/guarantees.md`](../../../docs/guarantees.md).

## Contract

Four logical identities remain distinct:

- `session_id` identifies the logical agent session across turns.
- `turn_id` identifies the logical unit whose ownership is fenced.
- `operation_id` identifies an idempotent transition or external effect across
  delivery attempts.
- `effect_id` identifies one destination mutation across delivery attempts.

Temporal Workflow, run, Activity, attempt, Worker, and process identifiers are
delivery observations. They never replace a logical identity. An owner is the
pair `(turn_id, generation)` and proves authority with an opaque capability;
protocol records contain only its SHA-256 digest.

Lifecycle and authority are separate axes. Lifecycle is one of `claimed`,
`starting`, `running`, `completing`, `succeeded`, `canceled`, or `unresolved`.
Authority is `current` or `revoked`.

| Operation | Required lifecycle transition | Authority result | Actor |
| --- | --- | --- | --- |
| `claim` | absent → `claimed` at generation 1 | current | coordinator |
| `begin_start` | `claimed` → `starting` | current | executor |
| `register` | `starting` → `running` | current | executor |
| `attach` | `starting`, `running`, or `completing` → same | unchanged | executor |
| `replace` | nonterminal → `starting` at the next generation | new owner current | coordinator |
| `observe_progress` | `running` → `running` | current | executor |
| `publish_effect_receipt` | `running` → `running` | current | executor |
| `publish_result` | `running` → `completing` | current | executor |
| `complete` | `completing` → `succeeded` | revoked | executor |
| `cancel` | nonterminal → `canceled` | revoked | coordinator |
| `mark_unresolved` | nonterminal → `unresolved` | revoked | coordinator |
| `record_stop_delivery` | revoked generation or canceled turn → same | revoked | coordinator |
| `acknowledge_stop` | revoked generation or canceled turn → same | revoked | coordinator |

Every request carries a stable `operation_id` and request hash. Every response
contains an immutable, operation-specific receipt: registration and attachment
receipts name an exact process or provider identity; replacement receipts name
both generations; effect receipts name an `effect_id`, destination namespace,
declared capability, and outcome; result/completion receipts link candidate and
terminal state; cancellation, ambiguity, and stop receipts preserve their
reason or exact target. Repeating the same operation and request hash returns
the original receipt without applying the transition again. Repeating an
`operation_id` with changed content is a conflict.

## Validation boundary

The JSON Schemas enforce versioning, closed record shapes, legal lifecycle
pairs, actor roles, accepted/rejected decision consistency, digest-only
capabilities, and evidence provenance. They deliberately do not pretend that
JSON Schema can derive durable state.

Bindings and conformance implementations must additionally enforce:

- the actor generation and capability digest equal the durable current owner;
- raw owner capabilities are generated with cryptographically secure,
  high-entropy randomness and their digests are compared in constant time;
- `replace` increments the durable generation exactly once;
- every non-`replace` transition preserves generation and
  `owner_capability_digest` across actor, prior state, next state, and durable
  authority; rejected operations preserve the complete state;
- the receipt request hash equals its enclosing transition request hash;
- self-transitions preserve the complete state, not only its lifecycle class;
- a repeated operation returns the original receipt only when its request hash
  matches, and otherwise conflicts;
- registration/attachment identities, replacement generations, effect IDs,
  completion links, and stop targets equal their enclosing and durable records;
- evidence observation identities equal the episode identities, including
  `effect_id` when present; a mismatch is conformance-invalid even though both
  records are structurally valid in isolation;
- sequence values are monotonic within their declared stream;
- Temporal Activity attempts are compared only within one stable Activity ID;
- timestamps parse as real UTC instants, not merely timestamp-shaped strings;
- duplicate JSON object keys are rejected before schema validation; and
- every artifact and history path is confined before archive/extraction, IDs
  are never interpreted as paths, and repository metadata is never fetched
  without an allowlist plus the recorded digest.

An `authority_check` is therefore an observed result produced by a binding
after consulting durable authority. `coordinator` means the independently
authenticated application coordinator authorized a coordinator-only operation;
`current` means the executor generation and capability were current; `stale`
and `revoked` are rejected owner checks. The transition schema prevents a
record that labels the result `stale` or `revoked` from also claiming the
operation was applied. Successful/replayed operations carry typed receipts;
rejected operations carry typed rejection results and cannot carry a success
receipt. The schema does not calculate freshness or cross-record equality from
two numbers or strings.

All free-form strings and identifiers are non-secret metadata. Producers must
redact or reject credentials before persistence. A provider session identity
must be a non-bearer identifier or an application-owned digest/reference, never
an API token or resumable bearer credential. Raw owner capabilities and
provider credentials are forbidden in transitions, events, evidence, Workflow
history, and generated artifacts. This is a producer/binding policy because
JSON Schema cannot recognize every secret embedded in an otherwise valid
string without semantic heuristics.

## Files and reproduction

- `schema/identity.schema.json` defines logical and delivery identities.
- `schema/transition.schema.json` defines the state machine and decisions.
- `schema/event.schema.json` defines closed protocol observations whose
  free-form values remain subject to the producer secret policy above.
- `schema/evidence.schema.json` defines a conformance episode and provenance.
- `fixtures/valid/` and `fixtures/invalid/` are the portable contract corpus.
- `schema/schema-manifest.json` pins the exact schema bytes.

From this directory, regenerate the manifest and validate the corpus with:

```bash
go generate ./...
go test .
```

`go generate ./...` is deterministic: it hashes the four schema files in a
fixed inventory and emits sorted JSON keys. A clean regeneration must leave
`schema/schema-manifest.json` byte-for-byte unchanged.

## Responsibility split

Temporal recovers the Workflow and redelivers Activity procedures. Application
code owns stable identity, fencing, receipts, cancellation state, and bounded
recovery. The destination owns whatever conditional-write or idempotency
protocol makes a committed effect safe to retry. A provider session ID can help
resume context, but is not turn authority or an effect receipt.
