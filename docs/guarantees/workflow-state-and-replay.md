# Workflow state and replay

Temporal recovers recorded procedure. It does not recover anything the
application kept outside Event History.

Back to the [guarantee summary](../guarantees.md).

## Workflow state durability

- **Temporal:** Event History and replay preserve recorded Workflow procedure.
- **Your application:** deterministic Workflow code and compatible evolution.
- **Your destination:** Temporal Service persistence and availability.
- **Evidence:** [captured history replays](../../experiments/worker-death/README.md#preserved-milestone-run);
  an incompatible timer change was rejected with `TMPRL1100`.

## Completed native agent steps during replay

- **Temporal:** Event History restores completed model and tool Activity results
  and Workflow decisions.
- **Your application:** Workflow code and plugin parameters must remain
  replay-compatible; stable logical identity must not use Activity attempt IDs.
- **Your destination:** an available compatible Worker, and destination safety
  for any retried incomplete Activity.
- **Evidence:** model-completion and post-result Worker-death tests plus
  captured-history replay in
  [Finding 0008](../findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md).
  An incomplete model Activity retried, while completed model and tool
  Activities were not reissued after `result_built`.
