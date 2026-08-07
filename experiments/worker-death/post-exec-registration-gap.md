# Post-exec / process-registration gap

## Question

What can an Activity retry infer when the durable executor is still
`launch_pending`, but the child process has already executed and has not yet
registered itself?

## Hypothesis and invariant

The durable work-store state alone cannot distinguish this live child from the
pre-`exec` phantom reproduced by the earlier launch-gap experiment: both expose
generation 1 as `launch_pending` with PID 0. A separate process-discovery signal
or an explicit replacement policy is required.

For either tested policy, one logical session must eventually expose one
accepted effect and one accepted outcome. The attach arm must reuse the exact
known-live child. The replacement arm must reject the old child's delayed
generation capability before registration, accept only generation 2, and
observe that the old PID/start identity disappears.

## Exact boundary

The generation-1 child decodes and removes its private launch request, obtains
its Linux PID/start identity, and arrives at `before-registration/1`. It has
executed but has not called `RegisterProcess`, progress, an effect, or
completion. Activity attempt 1 separately arrives at
`activity-before-first-heartbeat/1` after its launcher returns.

The controller validates the child arrival's session, generation, owner-token
hash, actor, PID, and process-start identity. It also checks that the work store
still contains one PID-0 `launch_pending` executor. Only then does it send
`SIGKILL` to Worker 1 and re-read the child's PID/start identity to prove the
child survived.

No sleep selects the failure boundary. Timeouts only bound a failed run.

## Arms and expected observations

| Arm | Retry decision | Expected observation |
| --- | --- | --- |
| `attach-control` | Attach to generation 1 | Attempt 2 launches no child. Releasing the discovered child lets it register, produce one effect/outcome, and exit. |
| `fenced-replacement` | Atomically replace pending generation 1 | Generation 2 registers and completes while generation 1 remains alive. Releasing generation 1 produces a stale-registration rejection carrying the same PID/start identity; the child then exits. |

The attach arm is a positive control for a healthy, discoverable child. Read
together with the earlier pre-`exec` phantom control, it demonstrates why
`launch_pending` cannot itself prescribe attach or replace.

## Responsibility and authority boundaries

- Temporal detects the lost Activity through heartbeat timeout and delivers
  attempt 2. It does not discover, attach to, fence, or terminate the child.
- The lab's child-side barrier is a trusted same-host discovery channel. It is
  experimental evidence, not a production registry.
- The application store supplies conditional generation replacement and rejects
  a stale owner capability at registration before progress or effects.
- The detached child inherits no Worker environment. Its raw generation token
  exists in the mode-0600 database/request and is removed from the request file
  after decoding; portable evidence contains only its hash.
- The rejected generation token revokes authority only at the cooperative work
  store API. This does not revoke arbitrary credentials already copied into a
  process or effects sent to a destination that ignores the fence.
- Cleanup evidence is single-host Linux PID plus process-start identity. The
  experiment observes cooperative exit after stale registration rejection and
  uses exact-identity `SIGKILL` only as failure cleanup.

## Success, failure, and falsifier

A run is invalid if the child is absent at the child barrier, its identity does
not survive Worker death, the store records a process before the boundary, or
Temporal does not deliver attempt 2.

The conclusion is falsified if attach launches a competitor, either arm accepts
more than one effect/outcome, replacement accepts generation-1 registration,
the Workflow result differs from the application outcome, the stale child can
reach progress/effects, or its exact PID/start identity remains after the
registration rejection.

## Preserved evidence

The final v3 matrix contains three fresh trials per arm. All six preserved
`pre-kill-state.json` snapshots show the live child alongside the same PID-less
`launch_pending` store state, and all six verdicts are valid:

- [attach-control trial 1](evidence/post-exec-gap-20260806-v3-attach-control-trial-1/verdict.json)
- [fenced-replacement trial 1](evidence/post-exec-gap-20260806-v3-fenced-replacement-trial-1/verdict.json)

The other trials use the same prefix with `trial-2` and `trial-3`. V1 is
excluded from the final claim because its manifest reported the prior protocol
label. V2 corrected that label but is excluded because it predates the
standalone pre-kill snapshot. Both sets remain preserved rather than rewritten.

See [finding 0005](../../docs/findings/0005-launch-pending-does-not-identify-process-reality.md)
for the observed conclusion and limitations.

## Run

```bash
make build
./bin/worker-death-experiment \
  --scenario post-exec-gap --arm all --trials 3 \
  --run-id post-exec-local
```
