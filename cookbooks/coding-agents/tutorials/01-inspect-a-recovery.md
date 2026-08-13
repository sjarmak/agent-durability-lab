# Inspect a recovery

Start from the failure, not the retry policy. In the selected scenario, attempt 1 commits the tool
effect and the Worker is replaced before Activity completion is durable. The unsafe implementation
fresh-launches another executor, so the same logical effect commits twice.

In the explorer, compare the three episode rows before selecting details:

1. **Unfaulted** calibrates the apparatus with one effect.
2. **Unsafe** proves the exact fault can distinguish the broken policy with two effects.
3. **Protected** reuses stable identity, checks current authority, and attaches to the existing
   execution, retaining one effect.

Then inspect four views in order:

- the normalized timeline explains the event sequence;
- logical identities stay stable while Temporal attempts and process identities change;
- authority and destination receipts show who may act and what committed;
- native Temporal history and the raw trial record remain the oracle inputs.

The lesson is not “Temporal retried safely.” Temporal durably redelivered the procedure. The
application supplied identity and authority, and the destination supplied effect enforcement.
