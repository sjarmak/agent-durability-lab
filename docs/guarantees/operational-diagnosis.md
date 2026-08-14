# Operational diagnosis

Temporal shows you task and Workflow state. It cannot tell you whether the
external agent is working or wedged.

Back to the [guarantee summary](../guarantees.md).

## Distinguish a wedged application from a healthy retry

- **Temporal:** exposes task and Workflow state, not external-process truth.
- **Your application:** correlate retry attempt, launch lifecycle, PID and
  process identity, progress, and outcome age.
- **Your destination:** process registry and clock or telemetry integrity.
- **Evidence:** the launch-gap control records attempt-2 application code
  attached to `launch_pending` with PID 0. Server-visible heartbeat evidence and
  operator policy for wedged `running` work remain unresolved.
