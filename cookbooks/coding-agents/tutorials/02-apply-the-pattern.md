# Apply the pattern

Activity retry is delivery, not coding-agent identity. Carry one stable session, turn, operation,
and effect identity across every delivery. Keep the owner generation and capability in durable
application state; keep raw capability material out of history and evidence.

At each external boundary:

1. Authenticate the coordinator or current executor.
2. Classify the request as accepted, replayed, stale, revoked, canceled, or conflicting.
3. Use start-or-attach for the external executor.
4. Require the destination to condition effects on stable identity or current authority.
5. Fence the obsolete owner before authorizing a replacement.
6. Revoke authority before stopping the process tree.
7. Publish results and acknowledge completion only for the current generation.

Choose the focused recipe that matches the boundary:

- native loop ownership and replay: `01-native-agent-loop`;
- effect identity and destination protocols: `02-effect-safe-tools`;
- external CLI process/session ownership: `03-external-cli-ownership`;
- cancellation and verified cleanup: `04-cancellation-and-cleanup`;
- sandbox/resource reconciliation: `05-sandbox-lifecycle`;
- bounded recovery and poison isolation: `06-bounded-recovery`.

Preserve the unsafe control when adapting a recipe. A protected pass without a distinguishing
negative control is a demo, not recovery evidence.
