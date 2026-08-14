# Finding 0010: Direct Claude Activity retry duplicates turns and effects

Nine faulted trials put `claude -p` in a retryable Activity. Each started a
second Claude session with a new session ID and applied the effect twice.
Temporal accepted one outcome. Three unfaulted trials applied once.

**Status:** observed in 12 admitted authenticated Claude Code trials

**Versions:** Claude Code `2.1.226`; Temporal Server `1.31.2`; Temporal CLI
`1.8.0`; Temporal Go SDK `1.47.0`; Go `1.25.12`; Linux amd64

**Source identities:** Claude binary
`4e9bec1177ce9690e8bd988b710ac24105e70da428dd094c5adcbbe786a55555`;
Worker and evidence adapter
`e5cb3829d859ebc6108c3063c875c2ce68a5ef885dfae8263d72079376da2540`;
controlled effect
`0f952a4cb12085f512df068e9f6518946e1106ee87d305c69a155d75dfe84b7b`;
pre-registration launcher
`a27adef375851caaf4662529e542fd2fc74543c5be749e0476320650dd100c1b`

## Claim

Putting `claude -p` directly inside a retryable Temporal Activity does not make
the vendor turn or its workspace effects durable as one execution. When the
Worker died at any of three exact boundaries, Temporal correctly redelivered
the Activity, but the application started a fresh Claude process and Claude
assigned it a fresh session. Both deliveries applied the same stable logical
effect. Temporal accepted one Activity outcome while the independent
destination and workspace recorded two physical effects.

All three unfaulted trials had one Activity attempt, one Claude session, one
physical effect, and one accepted outcome. All nine unsafe trials had two
Activity attempts, two distinct Claude sessions, two physical effects, and one
accepted outcome. The common oracle classified the unfaulted runs
`valid-pass` and every unsafe run `valid-fail / duplicate_physical_effect`; no
v5 run was invalid.

This is evidence against direct CLI relaunch as a durable-session integration.
It is not evidence that Claude session resumption, application-owned
start-or-attach, or generation fencing works; those are separate experiment
arms.

## Observed failure boundaries

- At `process-created-before-vendor-registration`, the Worker was killed after
  child creation but before `exec` could start Claude or expose a vendor session
  ID. The original child was then released and completed its effect; Activity
  retry independently launched a second Claude session and effect.
- At `tool-effect-before-activity-completion`, the first Claude-controlled
  effect was durably visible while its process remained blocked. Worker loss
  caused a second delivery, session, and effect.
- At `final-output-before-activity-completion`, the first Claude process had
  exited successfully and emitted one schema-validated terminal result, but
  the Activity had not returned. Worker loss caused the entire CLI turn and
  effect to run again.

Across the suite, 21 Claude invocations emitted 21 distinct vendor session IDs
and 21 successful structured terminal results. The accepted Workflow result
was `EFFECT_COMPLETE` in every run. In the nine unsafe runs, that single
accepted result coexisted with two independently observed physical effects.

## Responsibility split

- Temporal durably records Workflow state, detects the lost Worker through the
  Activity heartbeat timeout, redelivers the Activity, and records one accepted
  completion. That is delivery recovery, not external process attachment or
  effect deduplication.
- The application chooses to launch a new CLI on every delivery. In this unsafe
  arm it supplies stable logical IDs only for observation; it supplies no
  start-or-attach registry, owner generation, fence, resume policy,
  idempotency, or reconciliation.
- Claude Code starts a fresh vendor session for each direct invocation and
  executes the one allowed Bash tool. Transcript identity does not authorize a
  workspace writer and is not a destination transaction.
- The fixture filesystem and BoltDB journal establish physical effect count.
  A production destination would need to atomically deduplicate, fence, or
  reconcile its own mutation protocol.

Temporal Activity retry therefore must not be described as exactly-once tool
execution. Vendor transcript resumption alone would not establish that
property either.

## Scope — what this does not show

The trials used Claude Haiku through a Claude Max account, a local Git fixture,
and a local BoltDB destination. They ran on one Linux host and killed the Worker,
not the host or Temporal Service. The pre-registration child was deliberately
released before retry to make both possible launches observable in a fixed
order. Three trials per boundary show repeatability, not a production failure
rate.

The CLI was restricted to Bash plus one exact allowed command, and terminal
admission used Claude's native JSON Schema output. The authenticated profile
directory is external environment state and is not copied into evidence. No
credential value is recorded. This arm does not call `--session-id`,
`--resume`, or `--continue`, and it does not test interactive PTYs, concurrent
resume, cancellation, cross-host discovery, vendor transcript completeness,
or protected ownership protocols.

## Evidence and what would change this conclusion

The admitted evidence is
[`claude-direct-20260808-v5`](../../experiments/durable-vendor-sessions/claude-direct/transport/README.md#current-package).
It contains 12 run directories, 21 raw Claude JSONL streams, full Temporal
histories, exact barrier and process identities, independent workspace and
destination state, common-oracle verdicts, and per-run raw inventories. An
independent post-run check matched all 345 inventoried file sizes and SHA-256
hashes. V5 has three unfaulted passes and nine expected unsafe failures, with
three admitted trials at each fault boundary.

V1 is preserved but not admitted. Eight runs finalized before a third
unfaulted result used `EFFECT_COMPLETE.` rather than the harness's overstrict
free-form token; the corrected harness uses schema-validated structured output.
V2 completed all 12 trials with the same verdicts but pinned the `claude-1`
account wrapper as the agent entrypoint. V3 invoked the actual Claude binary
directly with the same account's isolated config directory, so the manifest
bound the vendor executable. Review then tightened terminal admission to
require `subtype: success` and `is_error: false`. All 21 v3 terminal events
already met those conditions, but the Worker hash changed. V4 repeated the
suite against the hardened source. Static analysis then required lower-case Go
error strings; that non-semantic edit changed the binary hash. V5 repeated the
suite and is the admitted source-matched run. No earlier evidence was deleted
or rewritten.

The Git-safe transport preserves all five roots as deterministic archives with
separate manifests. Across v1-v5 it binds 2,206 files and 56 finalized run
inventory/verdict chains; a clean-clone test restored every file and mode and
verified all 57 nested Git repositories. This transport result establishes
artifact portability only. It does not reclassify a run or strengthen the
behavioral claim.

The finding is falsified if fresh source-pinned admitted trials without attach,
resume, fencing, idempotency, or reconciliation produce one Claude session and
one physical effect after Worker loss at all three boundaries; if the fault is
not bracketed by the recorded barrier and replacement attempt; if any raw
inventory or process identity fails verification; or if the accepted result
does not correspond to the independently observed workspace and destination
state.
