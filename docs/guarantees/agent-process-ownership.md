# Agent process identity and ownership

Temporal history contains no OS process. Every property on this page is
supplied by application state, and the negative controls show what happens
without it.

Back to the [guarantee summary](../guarantees.md).

## Stable logical agent identity

- **Temporal:** no automatic mapping to an external process.
- **Your application:** stable session key, owner generation, and durable process
  or session registry.
- **Your destination:** store durability plus a trustworthy discovery or routing
  mechanism.
- **Evidence:** [one session across two attempts](../../experiments/worker-death/evidence/milestone1-20260806-v3-reattach/application-state.json);
  an [unregistered child discovered by exact local identity](../findings/0005-launch-pending-does-not-identify-process-reality.md),
  single host only.

## No duplicate launch on retry

- **Temporal:** no.
- **Your application:** atomic start-or-attach plus explicit lifecycle, and a
  policy informed by process discovery or fenced replacement.
- **Your destination:** store serialization, discovery trust, and executor
  registration before effects.
- **Evidence:** [a registered child reattaches](../../experiments/worker-death/evidence/milestone1-20260806-v3-reattach/application-state.json);
  a [known-live unregistered child reattaches](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-attach-control-trial-1/application-state.json);
  a [phantom pending launch is replaced](../../experiments/worker-death/evidence/launch-gap-20260806-v3-fenced-recovery/application-state.json).
  `launch_pending` alone cannot choose the policy.

## Liveness after pre-launch Worker death

- **Temporal:** Activity retry only. Temporal does not know whether `exec`
  happened.
- **Your application:** pending-launch detection and fenced conditional
  replacement.
- **Your destination:** an available Worker, process launcher, and work store.
- **Evidence:** [blind attach stalls on a phantom](../findings/0002-launch-decision-is-not-process-liveness.md);
  fenced recovery completes in two preserved trials.

## Stale-writer rejection

- **Temporal:** no.
- **Your application:** monotonic attempt check for replacement; generation or
  token and `running`-state checks at registration and every authoritative
  mutation.
- **Your destination:** must enforce or delegate the fence.
- **Evidence:** fenced run events 15 to 18 reject the stale effect and
  completion;
  [post-`exec` replacement rejects the old child's exact PID and start registration](../../experiments/worker-death/evidence/post-exec-gap-20260806-v3-fenced-replacement-trial-1/application-state.json).
