# Agent Durability Lab operating rules

This repository investigates whether an agent application remains correct after
Temporal recovers. It is an evidence lab, not a collection of "agent in a
Workflow" demos.

## Start and finish work

- Run `bd prime`; use Beads for durable work tracking and `bd remember` for
  project memory. Do not create markdown task lists or `MEMORY.md`.
- State the invariant, failure boundary, success/failure oracle, and falsifier
  before implementing an experiment.
- Before calling work complete, run `go test -race ./...`, preserve the evidence,
  update the guarantee page under `docs/guarantees/` plus the one-line verdict in
  `docs/guarantees.md`, update the finding and its line in `FINDINGS.md`, and
  close its bead.
- New findings follow `docs/findings/TEMPLATE.md`: abstract under forty words,
  counts before method, one `## Scope — what this does not show` block, and one
  statement of what would change the conclusion.
- `docs/decisions/0004-protect-the-unattributed-column.md` binds external-facing
  prose: keep the "No" cells unattributed, keep unfavorable measurements at full
  precision, and show the unsafe control before the protected one.
- Do not commit, push, or publish external artifacts without explicit approval.

## Evidence rules

- A passing demo is not evidence. Include a negative control capable of failing
  the invariant, use an exact barrier instead of a timing guess, and repeat
  concurrency-sensitive trials.
- Never delete, rewrite, or hide failing raw evidence. If a harness or test is
  wrong, preserve the original result and add the corrected run with an
  explanation.
- Record stable identities, ownership generation/token, Temporal attempt,
  Worker/process identity, event sequence, and UTC timestamps.
- Separate observation from inference. Say which guarantee comes from Temporal,
  application code, or the external destination, and state what would falsify
  the conclusion.
- Do not say "exactly once" unless the destination protocol and the evidence
  establish it. Temporal Activity retries alone do not make effects exactly once.

## Temporal and Go rules

- Workflow code must remain deterministic: no direct IO, subprocesses, native
  goroutines/channels, wall-clock time, randomness, or unordered map decisions.
  Use SDK Workflow APIs and keep external work in Activities.
- Treat replay compatibility as an acceptance criterion. Replay captured
  histories when Workflow code changes; use supported versioning mechanisms for
  intentional behavior changes.
- Activity attempts are delivery attempts, not agent identity. Use stable
  application-level operation/session identity across retries. Never use a task
  token or PID as the durable logical identity.
- Propagate `context.Context`, heartbeat genuinely long Activities, target
  cancellation to the current fenced owner, and return errors with context.
- Write idiomatic, race-free Go. Prefer immutable values and small packages;
  avoid package-level mutable state. Use channels for in-process coordination,
  not as a substitute for durable cross-process state.
- Follow TDD: RED, GREEN, REFACTOR. Tests must cover unit, integration, and the
  critical process/service path with at least 80% coverage. Do not use
  `time.Sleep` to open failure windows.

## Scope and structure

- Put a one-off mechanism beside its experiment first. Move it to `internal/`
  only when it expresses a real shared boundary or has a second use.
- Prefer simplifying an experiment over adding a framework. Product-like code
  belongs here only when an experiment needs it or repeated evidence justifies
  reuse. No UI, generic agent framework, or deployment platform without a
  research requirement.
- Each experiment README answers: question, invariant, failure boundary, oracle,
  run command, evidence location, observed result, responsibility split, and
  falsifier.
- Write supported claims in `docs/findings/`; keep unresolved questions explicit.
  Architectural choices with lasting consequences go in `docs/decisions/`.

## Failure-mode preventions

- Do not stop after reporting review findings: a request to review authorizes fixing in-scope findings and rerunning gates; ask approval only for commits, pushes, publication, destructive actions, or scope expansion.
- Do not infer that `WaitForCancellation=false` forbids an `ActivityTaskCanceled` event; it only means the Workflow does not await that event, and prompt cancellation may still record it.
- Do not scope an exact logical fault barrier only to Activity attempt 1. A delivery attempt can time out while waiting for the barrier; later attempts must still commit any required, uncommitted boundary before proceeding.
- Give intentionally held barrier servers a bounded graceful shutdown followed by an explicit, verified force-close; retain any completed episode evidence even when teardown reports an error.
- Do not put a server response deadline on an exact long-poll barrier whose response is intentionally held until fault release; bound it with the caller context, experiment deadline, and verified shutdown instead.
- A reserved Activity task queue does not reserve Workflow-task capacity inside one large direct-Activity Workflow; bound admitted concurrency when a latency-critical control path shares that Workflow's event stream.
- Do not place unrelated fan-out items in one process-shared Bolt store; its file lock becomes a hidden global barrier. Isolate stores per logical item while keeping competing generations of that item on the same fenced store.
- Serialize the complete dependency call per logical item before deriving retry ordinals and parent request IDs; reserving lineage before an overlapping attempt finishes creates invalid retry ancestry.
- Do not hand an exact retry-sensitive boundary through a one-shot channel alone. Cache the stable boundary identity and let every Activity attempt observe the same value without releasing the protected process.
- Schedule latency-critical control work before enqueuing a bulk healthy cohort, and size the outer Workflow budget from repeated worst-scale recovery evidence rather than the happy path.
- Treat both `ENOENT` and `ESRCH` from procfs identity/state reads as an exited process; otherwise normal exit races can invalidate live recovery runs.
- Do not treat a generic `recovery_observed/observed` event as silent-progress detection; require the fenced authority revocation or a failed deadline event.
- Scope Temporal Activity-attempt monotonicity to one stable Activity ID; a new Workflow-scheduled Activity legitimately restarts at attempt 1 even when it continues the same logical operation.
- Validate detection and control-lane latency after accumulated worst-scale recovery work, and reserve dispatch margin inside the registered bound instead of using the isolated-run timer as the bound.
- Partition race-and-coverage-instrumented real-service suites into independent package processes, merge only complete compatible profiles, and never admit a partial profile from an aggregate package timeout.
- Pin coverage test package sets explicitly; recursive package globs silently expand when a new benchmark generation is added and can rerun unrelated real-service suites.
- Do not repeat a fenced owner transition when a replacement Activity is redelivered; inspect the durable active generation and attach to the already-authorized replacement.
- Do not emit or score `authority_revoked` before the external authority store commits the replacement generation; a scheduled revocation is not yet an enforced fence.
- Do not validate archive paths with clean/absolute checks alone; reject parent prefixes and backslashes before confined extraction, or cross-platform traversal can bypass the manifest boundary.
- Do not drain bytes after tar end markers as harmless padding; require decompressed EOF at the parsed archive boundary, or unmanifested payload can pass verification.

## Default commands

```bash
bd ready
go test -race ./...
make coverage
```
