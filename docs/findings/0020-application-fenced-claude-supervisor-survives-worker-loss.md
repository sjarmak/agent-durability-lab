# Finding 0020: An application-fenced Claude supervisor survives Worker loss

A supervisor outside the Worker owned generation and capability. Fifteen
protected runs passed at four exact boundaries with one process, effect, and
workspace outcome each. Matched resume-only controls duplicated all nine
faulted effects.

**Status:** observed in 27 admitted source-pinned hermetic trials and 27
source-matched authenticated Claude Code trials

**Versions:** hermetic Claude protocol `1.0`; Claude Code `2.1.227`; Temporal
Server `1.31.2`; Temporal CLI `1.8.0`; Temporal Go SDK `1.47.0`; Go `1.25.12`;
Linux amd64

**Source identities:** hermetic Claude
`02c69076783b2570c6c2eff4c73c8898252bb63286f54adedcc66c7642de4134`;
fixed authenticated `claude-4` wrapper
`873d51ae31e56e87370b37eef9ce02a58e767bd055c8ea9dd9f1404f07e30988`;
delegated Claude Code `2.1.227` binary
`6832dc3f1797b890b71116e5f2dbbf9a83fd3d0498c235b4b0f9cd0e6e499ad6`;
hermetic/authenticated experiment harnesses
`be84e206eea3f2bf8f17f582554666cfaa9f340601da6f4369a7e48e30cc037a` /
`68ab400a6e5d695014420e2d874665d2916790dcb477a8b640e730725eff77db`;
hermetic/authenticated Workers
`4d86c464ffe2d543e1dac5c7f4b7a922acc1540a3d90ec210587434388aca02c` /
`321e16d4ff19eef11f78a3a89204bbab08f33f4429f756f14dcf81dd598bc422`;
hermetic/authenticated controlled effects
`8a2cbab72d82571bb2d99ec4e84187a6255f3113f47ba05c59d9ec02b497e056` /
`1355765e4d5c4dffb64638ea2cd50de916a4c73d64536857145f078bcdf3667f`;
shared pre-registration launcher
`a9785932404cd1b6885008c55cef86a8ccf024164c4d5e8d8218a6d5ee600130`

## Claim

Moving external-process authority outside a Temporal Worker and enforcing a
monotonic generation plus opaque capability at every effect and terminal
boundary prevents Worker redelivery from starting a competing Claude turn in
the tested single-host mechanism. A replacement Worker attaches to the exact
live authorized process/session. If replacement is required, the supervisor
commits the newer generation before launch; the previous generation cannot
regain authority when an old request arrives later.

The matched resume-only control did not have this property. Its nine faulted
runs retained one selected session UUID but each created two processes and two
physical effects. The protected suite passed all 15 runs with one process, one
authoritative destination effect, one workspace mutation, and one accepted
Workflow outcome per logical run.

This is not an exactly-once claim. The application-owned authority store and
the tested BoltDB/workspace protocols enforce current-owner and stable-effect
rules. Temporal Activity retry alone does neither.

## Observed boundaries

The protected population contained three clean runs and three trials at each
of four exact Worker-loss boundaries:

- `claim-committed-before-process-exec`;
- `process-created-before-vendor-registration`;
- `tool-effect-before-activity-completion`; and
- `final-output-before-activity-completion`.

At every fault boundary, generation 1 remained the durable current authority
and attempt 2 attached to its exact PID, process-start identity, process group,
selected vendor session, and result channel. The audit reconstructed 12 attach
decisions and no replacement launch. Across the complete protected population
it found exactly 15 processes, 15 authoritative destination effects, 15
workspace effects, and 15 accepted outcomes.

Separate race-instrumented process/service tests repeated the concurrency-
sensitive supervisor cases three times. They cover concurrent start-or-attach,
fenced replacement, failed-process automatic replacement, cancellation
revocation, ABA-style stale authority, real process cancellation, and real
process replacement. Cancellation commits the terminal revocation before
signaling the exact current process group; late effect or completion requests
then fail against durable authority. These tests are mechanism acceptance, not
part of the 15-run sealed Worker-loss population.

## Evidence and admission

Each admitted protected root has 15 finalized runs and 399 raw artifacts.
The independent disk audit did not trust suite summaries: it recomputed all 15
verdicts, replayed all 15 Workflow histories, verified every raw inventory, and
bound generation/capability to process registration, launched arguments, raw
Claude stream, effect request, destination receipt, workspace journal, and
accepted outcome. It found zero capability leaks in the recorded evidence.

Each source-matched resume-only root has 12 finalized runs and 345 raw artifacts.
Its independent audit recomputed three clean passes and nine
`valid-fail / duplicate_physical_effect` verdicts, replayed all histories, and
verified 21 processes, physical effects, and workspace effects for 12 accepted
outcomes. Thus the controls distinguish the protected result under the same
harness and provider entrypoint within each matched pair. The authenticated
Claude Code `2.1.227` pair reproduced the hermetic pair: all nine faulted
resume-only runs duplicated their physical effect, while all 15 fenced runs
passed with 12 recovery attachments, one process/effect/outcome per run, and
zero capability leaks. All 27 authenticated histories replayed.

The authenticated Git-safe
[`fenced-claude4-evidence-transport-v3`](../../experiments/durable-vendor-sessions/claude-direct/fenced-claude4-evidence-transport-v3/transport-index.json)
binds the rejected logged-out root, superseded complete v1/v3, and admitted
staticcheck-clean v4: 1,679 files, 8,593,538 uncompressed bytes, and 45
finalized run chains. Its index SHA-256 is
`b3d2b35dc3f79038e9e968529a828b172fbd502b5eec657e940a4572a7481535`.
The matched
[`resume-claude4-evidence-transport-v3`](../../experiments/durable-vendor-sessions/claude-direct/resume-claude4-evidence-transport-v3/transport-index.json)
binds superseded v1-v3 and admitted v4: 1,872 files, 7,856,513 bytes, and
48 run chains; its index SHA-256 is
`006e1d7544a34f4e1c123b2865a153e8300247249c9a26e64dcd4c480dd7e71a`.
Independent rebuilds produced byte-identical package files.

The source-pinned hermetic Git-safe
[`fenced-evidence-transport-v2`](../../experiments/durable-vendor-sessions/claude-direct/fenced-evidence-transport-v2/transport-index.json)
binds five preserved roots, 1,697 files, 8,706,372 uncompressed bytes, and 45
finalized run chains. Its index SHA-256 is
`d43a5463f0dcfd852744cbf52ca649f4898873985ea61a516c1438ce18f40c02`.
The separate
[`resume-hermetic-evidence-transport-v2`](../../experiments/durable-vendor-sessions/claude-direct/resume-hermetic-evidence-transport-v2/transport-index.json)
binds 936 files, 3,399,329 bytes, and 24 run chains; its index SHA-256 is
`df92cbcf453e596f24a34ee1ea62ed2f8b8e5dd2899de2918a6ea68e147a7bb5`.

The earlier protected roots remain part of the correction record. The
authenticated v1 attempt and hermetic v1 are rejected incomplete roots. V2
completed and passed its contemporary disk audit, but later review added the
running-harness hash and made fenced Workflow cancellation wait for durable
supervisor revocation; those source changes required v3. Static-analysis and
module-graph corrections then changed the evidence-bound build identities, so
v3/control-v1 were superseded by current-source v4/control-v2. No evidence root
was deleted or rewritten.

The authenticated correction record is also append-only. Fixed-profile v1
passed both audits but was superseded after a concurrent shared protocol
dependency changed the build identities. Resume v2 then passed behavior audit;
its first semantic audit report omitted the recorded harness hash. The report
was retained, the compatible auditor was corrected to require the harness for
current populations while still accepting uniformly legacy evidence, and a
corrected report was added. Because that package change altered the imported Go
build identity, source-matched v3 populations were run. Static analysis then
required capitalization-only error-message corrections, changing the package
identity once more; final v4 control/protected populations were run and
admitted. The earlier authenticated Git-safe packages remain preserved.

## Responsibility split

- Temporal durably records Workflow input and procedure, detects Worker loss,
  redelivers the Activity, and retains replayable history. It does not discover,
  attach to, or fence the external process.
- The application supervisor owns generation allocation, opaque capabilities,
  process/session registration, exact start-or-attach decisions, replacement,
  cancellation revocation, and conditional effect/result/completion acceptance.
- The external authority store serializes current-generation transitions. The
  destination and workspace protocols enforce the fence and stable logical
  effect ID where mutations occur.
- The hermetic Claude process supplies a source-pinned, deterministic
  `stream-json` session/effect protocol. Authenticated Claude Code `2.1.227`
  supplies the provider-compatibility observation for the tested fixed account
  wrapper and headless invocation.

## Scope and what would change this conclusion

The trials used one contended Linux host, a supervisor that remained alive
outside the killed Worker, local BoltDB, and either the hermetic process or
authenticated Claude Code `2.1.227` through one fixed account wrapper. They did
not kill the supervisor host, move execution across hosts, test supervisor-store
disaster recovery, exercise interactive PTY attachment, or establish behavior
across a Claude Code deployment/version change. The HTTP control endpoint is
ephemeral loopback and assumes a trusted same-user host boundary. Arbitrary
external destinations remain safe only if they enforce or correctly delegate
the generation/capability and idempotency protocol. The first authenticated
attempt through the plain logged-out binary remains preserved as rejected
evidence; it is not counted in the admitted population.

The claim is falsified if a source-matched protected trial starts two current
executors, accepts a stale/canceled effect, result, or completion, launches a
replacement before committing its newer fence, fails to attach at a declared
live-process boundary, disagrees with independently observed destination or
workspace state, leaks an authority capability into evidence, or fails Workflow
history replay. The bounded provider-compatibility observation is falsified if
fresh trials with the recorded Claude version and fixed-profile invocation do
not reproduce the protected result.
