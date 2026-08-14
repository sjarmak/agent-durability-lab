# External effects

Temporal provides no external exactly-once effect, and this lab does not claim
one. What worked was a stable effect ID plus a protocol the destination
supports.

Back to the [guarantee summary](../guarantees.md).

## External exactly-once effect

- **Temporal:** no.
- **Your application:** not claimed generically. The application supplies a
  stable effect ID and the destination-specific protocol.
- **Your destination:** must atomically apply or deduplicate the ID, or expose
  enough state for bounded reconciliation.
- **Evidence:** [all 18 Go unsafe trials recorded two effects despite one Temporal completion](../findings/0004-one-temporal-completion-can-hide-two-effects.md).
  The [Temporal-native agent loop](../findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md)
  independently duplicated all three unsafe tool effects while its
  SQLite-idempotent arms applied one.

## Retry duplicate suppression for idempotent API, database, and message

- **Temporal:** Activity retry only.
- **Your application:** stable idempotency, mutation, or message key across
  attempts.
- **Your destination:** must atomically store the key with the effect or receipt
  and retain it through retry.
- **Evidence:** [one effect in nine protected trials; two in all nine controls](../findings/0004-one-temporal-completion-can-hide-two-effects.md).

## Sequential retry reconciliation for non-idempotent API and Git

- **Temporal:** Activity retry only.
- **Your application:** query a stable correlation or marker before repeating,
  and reject conflicting content.
- **Your destination:** strongly consistent lookup plus serialized same-ID
  callers or worktree access.
- **Evidence:** [one effect in six protected trials; two in all six controls](../findings/0004-one-temporal-completion-can-hide-two-effects.md).
  Concurrent check-then-act is not covered.

## Duplicate message suppression

- **Temporal:** task delivery and retry are durable; destination deduplication is
  not.
- **Your application:** stable message ID on every retry.
- **Your destination:** the tested simulated destination atomically retains
  message-ID deduplication state. Real broker semantics are untested.
- **Evidence:** [three protected trials published one message; three controls published two](../findings/0004-one-temporal-completion-can-hide-two-effects.md).
  Broker acknowledgement loss and retention expiry remain unresolved.
