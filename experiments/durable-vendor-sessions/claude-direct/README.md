# Direct Claude CLI recovery controls

**Status:** observed in 24 admitted authenticated Claude trials
**Tracking:** `temporal_projects-5im.5`, `temporal_projects-5im.7`
**Observed CLI:** Claude Code 2.1.226 on 2026-08-08 and 2026-08-10
**Observed binary SHA-256:** `4e9bec1177ce9690e8bd988b710ac24105e70da428dd094c5adcbbe786a55555`

## Question

What happens when a retryable Temporal Activity invokes `claude -p` directly
and its Worker dies before vendor registration, after a workspace effect, or
after final structured output but before Temporal records Activity completion?

This is the deliberately unsafe control. It does not pass `--session-id`,
`--resume`, `--continue`, `--background`, or any application ownership token.
Each Activity delivery starts a fresh CLI process.

The resume-only arm asks a narrower follow-up: if the application chooses and
durably records an RFC 4122 UUIDv4 before Workflow start, invokes the first
delivery with `--session-id <uuid>`, and invokes every redelivery with
`--resume <uuid>`, does Claude preserve session identity, and does that identity
also prevent duplicate effects? The arm never uses `--fork-session` or
`--continue`.

## Invariant and falsifier

For one logical turn, at most one executor may advance the turn, one physical
workspace effect may be applied, and the accepted Workflow result must agree
with the independently observed workspace and destination.

The proposed negative control is falsified if admitted Worker-death trials show
one executor, one physical effect, one vendor session, and result/workspace
agreement despite the direct Activity retry having no attach, fencing,
idempotency, or reconciliation mechanism.

For resume-only, every provider event must report the precommitted UUID and
there may still be only one physical effect and one accepted outcome. The
effect-safety hypothesis is falsified by an admitted same-session retry that
applies the independently recorded effect twice. Session identity is not
treated as an ownership token or an exactly-once destination protocol.

## Failure boundaries

The unsafe suite injects Worker `SIGKILL` at three independently named points:

1. `process-created-before-vendor-registration`: a small launcher has created
   the child process but is blocked before `exec` replaces it with Claude, so no
   vendor session can exist yet;
2. `tool-effect-before-activity-completion`: the controlled workspace and
   destination effect are independently visible while Claude remains alive;
3. `final-output-before-activity-completion`: Claude has exited successfully,
   its one terminal `stream-json` result has been parsed, and the Activity is
   blocked immediately before returning it.

The launcher is only an exact failure-injection mechanism. It preserves the
child PID and process-start identity across `exec`, and it provides no attach,
resume, deduplication, or ownership behavior.

The controlled effect performs these operations in order:

1. commit a physical receipt to a BoltDB destination;
2. append the same physical attempt to the fixture repository's
   `effects.jsonl`;
3. arrive at `claude-tool-effect-committed`; and
4. block until the controller releases that exact barrier.

The controller sends SIGKILL to the exact Worker PID only after the selected
barrier is observed and the Worker process-start identity is revalidated. The
Claude, launcher, and controlled-effect children are intentionally not bound to
Activity cancellation. Temporal detects Worker loss through the Activity
heartbeat timeout and delivers attempt 2 to a second Worker. Each faulted trial
is admitted only after two independent physical effects are visible. The first
process is released before retry in the pre-registration case so its effect is
ordered before attempt 2; this makes the missing vendor-ID recovery gap exact
rather than timing-dependent.

No `time.Sleep` opens the failure window. Linux pidfds observe orphaned Claude
process exit without treating a reused PID as the original process.

## Oracle and admission

The model is not the oracle. A trial records and cross-checks:

- stable logical session, turn, and effect IDs;
- Temporal Workflow, Run, Activity, and attempt identity;
- Worker and Claude PID plus process-start identity;
- vendor-assigned Claude session ID from raw `stream-json` output;
- the exact executable, working directory, and raw argument vector, including
  one canonical `--session-id <uuid>` or `--resume <uuid>` pair in resume-only;
- exact pre-registration, tool-effect, or final-output barrier arrivals and UTC
  fault time;
- physical destination receipts and workspace journal entries;
- fixture before/after hashes and Git status;
- the accepted Workflow result and full Temporal history; and
- SHA-256 hashes for the Claude, Worker, launcher, and controlled-effect
  binaries.

`raw/raw-inventory.json` lists every other preserved file by relative path,
size, and SHA-256. The inventory hash is recorded in the common evidence input
and native journal, so the common manifest binds the raw provider,
process, history, workspace, and destination artifacts.

The shared append-only evidence writer and independent oracle classify the
unfaulted control as `valid-pass`. They classify the retry control as
`valid-fail` only when more than one physical effect exists for the same logical
effect. Missing streams, a wrong process identity, a missed barrier, an invalid
raw inventory, absent workspace state, or malformed history makes the trial
invalid rather than passing.

Resume-only admission also fails closed if the provider reports a session ID
other than the caller-selected UUID, if the raw invocation contains duplicate
or conflicting session controls, or if any captured Workflow history fails
replay. Both `valid-pass` and `valid-fail` outcomes are admissible when their
artifacts agree; admission does not assume which hypothesis should win.

The hermetic fake-Claude process/service E2E produces three valid passes and
nine valid failures with `duplicate_physical_effect`. The authenticated v5 run
produced the same verdicts with Claude Code 2.1.226: three unfaulted
`valid-pass` trials and three `valid-fail / duplicate_physical_effect` trials at
each fault boundary. Every unfaulted trial had one Activity attempt, one Claude
session, one physical effect, and one accepted outcome. Every faulted trial had
two Activity attempts, two distinct Claude sessions, two physical effects, and
one accepted outcome.

Across the suite, all 21 terminal Claude events reported successful
schema-validated `EFFECT_COMPLETE` output under 21 distinct vendor session IDs.
The independent verifier matched all 345 raw artifact sizes and SHA-256 hashes
against their per-run inventories. The admitted evidence is
[`claude-direct-20260808-v5`](transport/README.md#current-package); see
[Finding 0010](../../../docs/findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md).

The authenticated resume-only v5 run also completed 12 admitted trials: three
unfaulted `valid-pass` controls and nine faulted
`valid-fail / duplicate_physical_effect` trials, three at each boundary. All 21
Claude invocations reported the caller-selected UUID for their logical run, so
the suite used 12 provider session identities rather than 21. Every faulted run
nevertheless recorded two physical effects and one accepted Workflow outcome.
All histories replayed, and the verifier matched all 345 raw artifacts. The
admitted package is described under
[resume-only package](transport/README.md#resume-only-package); see
[Finding 0019](../../../docs/findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md).

## Responsibility split

- Temporal supplies durable Workflow state, Activity heartbeat timeout, and
  redelivery.
- The experiment application supplies logical identities, exact fault control,
  raw evidence, and the independent verdict.
- Claude Code supplies a vendor session and executes the allowed Bash tool, but
  the unsafe arm does not ask it to resume or attach; in the resume-only arm it
  preserves the caller-selected session identity across redelivery.
- The fixture filesystem and BoltDB destination determine whether effects were
  physically applied.

Temporal retry is not an exactly-once effect guarantee, and a resumable Claude
transcript would not by itself change the destination result.

## Run

Build and verify the hermetic control:

```bash
go test -race ./experiments/durable-vendor-sessions/claude-direct/...
```

With an authenticated Claude config directory, create a new evidence root and
run three trials per probe:

```bash
make build
CLAUDE_CONFIG_DIR=/path/to/authenticated/claude-config \
bin/claude-direct-experiment \
  --evidence-root experiments/durable-vendor-sessions/claude-direct/evidence/claude-direct-YYYYMMDD-v1 \
  --temporal-binary "$(command -v temporal)" \
  --worker-binary "$PWD/bin/claude-direct-worker" \
  --effect-binary "$PWD/bin/claude-direct-effect" \
  --launcher-binary "$PWD/bin/claude-direct-launcher" \
  --claude-binary "${CLAUDE_BINARY:-$(command -v claude)}" \
  --model haiku \
  --max-budget-usd 0.25 \
  --max-turns 3 \
  --recovery-mode resume-only \
  --trials 3
```

This produces three unfaulted trials and three faulted trials at each of the
three boundaries. The CLI runs with `--safe-mode`, `--permission-mode dontAsk`,
`--tools Bash`, one exact `Bash(<controlled-effect command>)` allow rule, and a
JSON Schema whose only valid status is `EFFECT_COMPLETE`. The harness does not
add a blanket permission bypass; account wrappers can add their own flags, so
the admitted v5 run invoked the actual Claude binary directly.

Omit `--recovery-mode`, or set it to `unsafe-fresh`, to reproduce the original
negative control. Resume-only requires enough turns for the resumed process to
observe the prior tool call and emit the schema-constrained result; the admitted
run used three. Activities heartbeat before local process setup, and the
15-second heartbeat timeout includes dispatch and procfs margin for a contended
development host. The resume v4 root is preserved but rejected because the
earlier 2-second timeout caused redelivery before the first process had emitted
its session receipt.

Evidence roots are append-only. A failed or invalid run must remain in place;
use a new versioned root for a corrected run.

V1 is preserved but not admitted: eight runs finalized before a third
unfaulted trial returned the correct free-form token with a trailing period,
exposing an overstrict prose parser. V2 corrected admission by using Claude's
native `--json-schema` output and completed all 12 runs, but pinned the
`claude-1` wrapper as its agent entrypoint. V3 used the same authenticated
account profile while invoking the actual Claude binary directly, removing
that provenance ambiguity. Review then made terminal admission fail closed on
Claude's `subtype` and `is_error` fields. All v3 terminal events were already
successful, but the source hash changed; v4 repeated the suite. Static analysis
then required a non-semantic error-message style correction, which changed the
binary hash once more. V5 repeated the suite and is the source-matched admitted
run. No earlier evidence was deleted or rewritten.

## Git-safe evidence transport

The raw v1-v5 roots contain nested `.git` directories and ignored database
files, so they must not be added directly to the outer repository. The
[deterministic evidence transport](transport/README.md) packages the complete
correction lineage as normal Git files, binds all 2,206 file artifacts and 56
finalized run inventories/verdicts, and restores the raw trees without creating
outer-repository gitlinks. The original local evidence remains unchanged.

Primary interface references:

- [Claude Code CLI usage](https://code.claude.com/docs/en/cli-usage)
- [Claude Code sessions](https://code.claude.com/docs/en/sessions)
- [Run Claude Code programmatically](https://code.claude.com/docs/en/headless)
