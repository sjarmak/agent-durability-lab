# Vendor CLI sessions

Four designs were tested against the same faults for both Claude Code and
Codex CLI: direct relaunch, resume-only, and application-fenced start-or-attach.
Only the fenced design held. Read the unsafe rows first.

Back to the [guarantee summary](../guarantees.md).

## Direct Claude CLI retry after Worker loss

- **Temporal:** Activity timeout and redelivery only. Temporal neither attaches
  to the surviving CLI nor reuses its vendor session.
- **Your application:** direct relaunch is unsafe. A protected design still needs
  stable turn identity, start-or-attach or fenced replacement, and destination
  effect safety.
- **Your destination:** Claude assigns a new session to each direct launch, and
  the destination must deduplicate, fence, or reconcile repeated effects.
- **Evidence:** [all nine authenticated unsafe trials launched two Claude sessions and applied two physical effects while Temporal accepted one outcome](../findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md);
  three unfaulted trials launched and applied once. The Git-safe transport binds
  all 2,206 v1-v5 file artifacts and 56 finalized run inventory and verdict
  chains without placing nested repositories in the outer Git index.

## Caller-selected Claude session resume after Worker loss

- **Temporal:** durably retains the selected UUID in Workflow input and
  redelivers the same stable Activity. It does not attach to or fence a vendor
  process.
- **Your application:** attempt 1 selects the UUID and attempt 2 resumes it.
  Session identity is not turn authority, so start-or-attach, current-owner
  completion, and destination safety remain application concerns.
- **Your destination:** Claude must retain the local same-working-directory
  transcript, and the destination must still deduplicate, fence, or reconcile
  effects.
- **Evidence:** [all nine authenticated resume-only trials reused one selected Claude UUID across both attempts but applied two physical effects while Temporal accepted one outcome](../findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md);
  all three unfaulted trials invoked and applied once. All 12 histories
  replayed, and the v1-v5 transport binds 1,759 files and 44 finalized run
  chains.

## Application-fenced Claude start-or-attach after Worker loss

- **Temporal:** durably records and redelivers the procedure and preserves
  replayable history. It does not own external-process authority or attachment.
- **Your application:** a supervisor outside the Worker atomically owns
  generation and capability authority, exact process and session registration,
  start-or-attach or fence-before-replace, cancellation revocation, and
  conditional effect, result, and completion.
- **Your destination:** the authority store and every mutating destination must
  enforce the current fence and stable effect identity. Live routing is
  single-host in this experiment.
- **Evidence:** [both the source-pinned hermetic and authenticated Claude Code 2.1.227 protected arms passed 15/15 runs at four exact boundaries](../findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md),
  each with one process, effect, and workspace outcome per run and 12 recovery
  attachments. Both matched resume-only controls duplicated all nine faulted
  effects. All 54 histories replayed. Supervisor and host loss, cross-host
  routing, and version change remain pending.

## Direct Codex CLI retry after Worker loss

- **Temporal:** Activity timeout and redelivery preserve one logical Activity but
  do not attach to or fence the surviving Codex process.
- **Your application:** a fresh relaunch is an unsafe control. Stable logical
  turn and effect identity and destination safety remain application
  responsibilities.
- **Your destination:** Codex assigns the thread after process start, and the
  destination must deduplicate, fence, or reconcile repeated effects.
- **Evidence:** [both matched unsafe run sets produced six passes and six duplicate-effect failures](../findings/0021-codex-thread-resume-is-not-turn-authority.md).
  Every post-effect fault applied twice while Temporal accepted one outcome. The
  hermetic and authenticated controls each recorded 21 processes, 18 threads and
  effects, and 12 replayed histories.

## Explicit Codex thread resume after Worker loss

- **Temporal:** durably retains the learned thread ID and redelivers the stable
  Activity. It does not make that thread current-owner authority.
- **Your application:** redelivery passes one canonical thread to
  `codex exec resume`. Start-or-attach, fencing, cancellation authority, and
  destination safety remain application concerns.
- **Your destination:** Codex must retain the thread transcript, and a mutating
  destination must still enforce idempotency, fencing, or reconciliation.
- **Evidence:** [both matched resume-only run sets preserved one logical thread per run but reproduced all six post-effect duplicate-effect failures](../findings/0021-codex-thread-resume-is-not-turn-authority.md).
  Each contained 12 replayed histories and 18 physical effects for 12 logical
  runs.

## Application-fenced Codex start-or-attach after Worker loss

- **Temporal:** records and replays the procedure, detects Worker loss, and
  redelivers. It does not discover, attach to, replace, or terminate Codex.
- **Your application:** an external supervisor serializes generation and
  capability authority, durably registers PID, start, and group plus learned
  thread identity, attaches callers, fences before replacement, revokes before
  cancellation signals, and verifies the original process group is empty before
  acknowledgment.
- **Your destination:** the authority store and every mutating destination
  enforce current generation and capability and stable effect identity. Routing
  and teardown are single-host Linux mechanisms here.
- **Evidence:** [the hermetic and authenticated Codex CLI protected arms each passed 27/27 runs across eight exact fault boundaries plus unfaulted controls](../findings/0021-codex-thread-resume-is-not-turn-authority.md),
  with 21 attachments, three replacements, three cancellations, zero capability
  leaks, and 27 replayed histories per environment. App Server, supervisor and
  host loss, cross-host routing, version change, and efficiency remain unproven.
