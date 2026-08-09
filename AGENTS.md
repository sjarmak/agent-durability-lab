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
  update `docs/guarantees.md` and the relevant finding, and close its bead.
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

## Default commands

```bash
bd ready
go test -race ./...
make coverage
```
