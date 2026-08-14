# Finding 0021: Codex thread resume is not turn authority

Codex `exec resume` preserved one logical thread and still duplicated six post-
effect faults. The fenced supervisor arm passed twenty-seven runs across eight
boundaries with twenty-one attachments, three replacements, and three
cancellations.

**Status:** observed in 51 source-pinned v12 hermetic and 51 matched
authenticated Codex CLI trials; the current tree includes later authenticated-
barrier hardening

**Versions:** hermetic Codex protocol `1.0`; Codex CLI `0.147.0`; requested model
`gpt-5.6-sol`; reasoning effort `low`; sandbox `workspace-write`; Temporal
Server `1.31.2`; Temporal CLI `1.8.0`; Temporal Go SDK `1.47.0`; Go `1.25.12`;
Linux amd64

**Evidence binary identities:** hermetic Codex
`7152df1d6e95db308b8181d5c3df00c187502d827c4bc5823ba05282af9489d7`;
fixed authenticated `codex-2` wrapper
`73962b1eac648401e8d48861bda01df1d591eb8433a361103b7939c2f269dfc5`;
delegated Codex CLI
`134063e133f0b4244fa3b251acf973d4fe4b4aeeacbdc135211bf480f59f1477`;
experiment harness
`025e6781a1b1530c42ef2e0847542515d86ea4dca615bab2b3779e10faae92ae`;
Worker
`8fd2b6553387436100be072f37b785425420881b7971bbe4063b15667c153a89`;
controlled effect
`b1b9a4b52da06165a3d412b9c93e4a301d4ef5817fb034285cc572c2f8ba022f`;
pre-thread launcher
`2fd6b433f760fcafc052fd2d42345fc4a04a5cbc670941512eb6fe0e0860c432`;
structured-output schema
`d25bb1661dc68e052ad690e2271e817c6a325e11963850991cd230517db5e249`

## Claim

Codex's persisted thread ID preserves transcript identity across a CLI resume,
but it is not authority over a running turn and does not make the turn's effects
safe under Temporal Activity redelivery. In the matched hermetic controls,
both fresh relaunch and explicit thread resume duplicated the physical effect
in every post-effect Worker-loss trial while Temporal accepted one outcome.

An application supervisor outside the Worker changed that result in the tested
single-host mechanism. It serialized a monotonic generation and opaque
capability, durably registered exact process identity before vendor-thread
observation, attached redelivered callers to a live authorized execution,
committed a newer fence before replacement, and revoked authority before
cancellation signals. The protected arm accepted no stale effect or outcome and
left no path-scoped process after teardown.

This is not an exactly-once claim. Temporal supplies durable procedure,
redelivery, and replay. The application authority store and controlled
destination/workspace protocols supply the tested ownership and effect safety.

## Evidence

The source-pinned v12 hermetic controls each contain 12 trials: three
unfaulted runs and three at process-before-thread, tool-effect-before-completion,
and final-output-before-completion. Each audit classified six runs as valid
passes and six as distinguishing `duplicate_physical_effect` failures. Unsafe
fresh recorded 21 processes, 18 threads/effects, 405 verified raw artifacts,
and 12 replayed histories. Explicit resume recorded the same process,
thread/effect, and replay counts across 429 artifacts; its redeliveries used the
durably selected thread rather than creating a new logical transcript.

The protected population contains 27 trials: three unfaulted plus three at each
of eight exact fault boundaries. Besides the three control boundaries, it
tests claim-before-exec, thread-before-durable-registration, concurrent
recovery, executing cancellation, and failure of an authorized process before
thread observation. All 27 runs passed. The independent audit verified 846 raw
artifacts, 30 processes, 27 threads, 24 physical effects, 21 attachments, three
fenced replacements, three cancellations, zero capability leaks, and 27
replayed histories.

At the threadless process-failure boundary, generation 1's process died after
its exact process receipt but before `thread.started`. Activity retry committed
generation 2 and completed with one effect. A rejected v11 population exposed a
harness cleanup error: the supervisor reported generation 1's expected
interrupted JSONL prefix even after durable replacement and successful Workflow
completion. The raw root was retained. The correction suppresses only an
ordinary execution error from a durably superseded generation; process-control,
unverified-termination, non-exit, and cancellation-acknowledgment errors remain
fatal. Focused race tests repeated both the recovered and fail-closed cases.

Cancellation review exposed a separate leader-only cleanup defect in historical
evidence. The final supervisor verifies the original process group after the
leader exits, force-closes surviving descendants, and refuses to acknowledge
cancellation when the group remains occupied or PID/group reuse is ambiguous.
The v12 protected population was followed by a procfs check that found no
process referencing its sealed root.

The serialized authenticated Codex CLI `0.147.0` population reproduced the
hermetic comparison under the fixed `codex-2` wrapper/profile and requested
`gpt-5.6-sol` model. Unsafe and resume-only each produced six valid passes and
six duplicate-effect failures across 12 runs, with 21 processes, 18
threads/effects, and 12 replayed histories. The protected arm passed 27/27 with
30 processes, 27 threads, 24 effects, 21 attachments, three replacements,
three cancellations, zero capability leaks, and 27 replayed histories. The
comparison therefore contains 102 admitted runs and 102 replayed histories.

The authenticated correction record is append-only. A repository-scoped
unsafe calibration and an accidentally overlapping retry population were
preserved and rejected; neither contributes a verdict. Earlier v9 populations
were complete but are superseded because they predate whole-process-group
teardown verification and the recovered superseded-generation cleanup rule.
Authenticated fenced v7 remains rejected after a Temporal persistence failure,
and v8 remains rejected for mixed binary provenance.

Six v4 Git-safe transports preserve the final and relevant correction lineage.
They bind 7,496 files, 38,186,197 uncompressed bytes, and 177 finalized run
chains; the latter includes superseded complete populations. Independent
package builds were byte-identical, every archive verified and restored, and
all six admitted restored bundles reproduced their disk audits and history
replays. The transport index SHA-256 values are:

- hermetic unsafe `b5ef901e81762bc30160dc1ddbdd24bb007ea908c09a4b1578096673d12a80a1`;
- hermetic resume `86379cd37aede290f2bc2ff9a0f655720e2b53265ce9a3e6a7df80ac19a4e13c`;
- hermetic fenced `fc9f758d26c3cddbe4a2495cbf25f5f6cb25609d2cd9ace194f4c5456ae152a0`;
- authenticated unsafe `0ea40ef0ab7521703dd8a21f20fb68714e86a2fe7f89a9bf3ad23fcdd73a6b9d`;
- authenticated resume `16f6a2439eab98a621852bec04bfe1458626440e0cdc27e7ece6c9243a902d69`;
  and
- authenticated fenced `92b685a6ae934d880136669c5c113dbdab0066f5c1447be7c4d948e899bd6075`.

## Responsibility split

- Temporal records Workflow input and procedure, detects Worker loss through
  Activity timeout, redelivers the Activity, and retains replayable history. It
  does not discover, attach to, or fence the Codex process.
- Codex CLI emits the vendor thread identity and supports transcript resume. It
  does not expose a caller-selected pre-launch thread ID in this tested surface,
  and resume is not treated as current-owner authorization.
- The application supervisor owns stable logical identity, generation and
  capability allocation, exact process/thread registration, start-or-attach,
  fence-before-replace, cancellation revocation, and conditional publication.
- The authority store serializes ownership. The destination and workspace
  enforce current generation/capability and stable effect identity where the
  mutation occurs.

## Scope and what would change this conclusion

The evidence comes from one contended Linux workstation, local Temporal dev
servers, BoltDB, and a loopback supervisor that survives the Worker. It does not
cover supervisor or host loss, cross-host discovery/routing, authority-store
disaster recovery, Codex App Server, interactive PTY attachment, deployment or
model change, or arbitrary external destinations and credentials. Because the
host was not controlled compute, these runs support no relative latency,
throughput, token-efficiency, or cost conclusion.

The admitted v12 binaries used the earlier trusted-loopback HTTP fault
boundary. The current tree additionally authenticates each pre-registered
point/session/generation/actor arrival with a random per-run credential and a
single-use nonce passed through an inherited descriptor. That hardening has
hermetic, race, and coverage validation, but it landed after the v12 freeze. A
new live population is required before describing the current source as fresh
authenticated Codex evidence.

The claim is falsified if a source-matched protected trial starts two current
executors, accepts a stale/canceled effect or outcome, attaches to a reused or
mismatched process identity, launches replacement before the newer fence
commits, acknowledges cancellation before the original process group is
verified empty, disagrees with independent destination/workspace state, leaks a
capability, or fails Workflow history replay. The authenticated compatibility
claim is falsified if the fixed wrapper, delegated CLI hash, requested model,
and recorded profile do not reproduce the protected result in fresh serialized
trials.
