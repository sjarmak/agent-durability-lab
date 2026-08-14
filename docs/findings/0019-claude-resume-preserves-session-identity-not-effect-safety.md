# Finding 0019: Claude resume preserves session identity, not effect safety

Nine faulted trials selected one Claude session UUID on attempt 1 and resumed
it on attempt 2. Both provider streams reported that UUID. Both deliveries
applied the effect. Session identity is not turn authority.

**Status:** observed in 12 admitted authenticated Claude Code trials

**Versions:** Claude Code `2.1.226`; Temporal Server `1.31.2`; Temporal CLI
`1.8.0`; Temporal Go SDK `1.47.0`; Go `1.25.12`; Linux amd64

**Source identities:** Claude binary
`4e9bec1177ce9690e8bd988b710ac24105e70da428dd094c5adcbbe786a55555`;
Worker and evidence adapter
`1efecc8376325c096d5047829f71ac8cde96dec9629188c86faa15807e514fca`;
controlled effect
`d1ba04c42341aef2412662f98316f096389cb6248622333ddb51d6f2e861d2ae`;
pre-registration launcher
`efb02b601f3af7103264d723e2ec6a63b292fbbda4c6f7bfe3d9ef58aba391cf`

## Claim

A caller-selected Claude session UUID survives Temporal Activity redelivery,
but transcript resumption does not make one logical turn or its external effect
execute once. In every admitted Worker-loss trial, attempt 1 invoked Claude with
`--session-id <uuid>` and attempt 2 invoked it with `--resume <uuid>`. Both
provider streams reported that selected UUID. Both deliveries still ran the
controlled effect, while Temporal accepted one Activity outcome.

The three unfaulted trials each had one Claude invocation, one session UUID, one
physical effect, and one accepted outcome. All nine faulted trials had two
Claude invocations in the same selected session, two physical effects for one
logical effect, and one accepted outcome. The independent oracle classified the
unfaulted runs `valid-pass` and every faulted run
`valid-fail / duplicate_physical_effect`.

This narrows the result from [Finding 0010](0010-direct-claude-activity-retry-duplicates-turns-and-effects.md):
new vendor-session creation was not the cause of the duplicate effect. Reusing
one transcript identity removed session ambiguity but did not supply turn
ownership, process attachment, stale-completion rejection, or destination
idempotency.

## Observed boundaries

- At `process-created-before-vendor-registration`, the Worker died after the
  launcher created the child but before Claude registered the selected UUID.
  The released child started that UUID and applied the effect. Temporal then
  redelivered the Activity; `--resume` found the same session and applied a
  second effect.
- At `tool-effect-before-activity-completion`, attempt 1 remained alive at the
  exact committed-effect barrier while attempt 2 resumed the same session and
  applied the effect again.
- At `final-output-before-activity-completion`, attempt 1 had emitted a
  schema-valid successful result before the Worker died. Attempt 2 resumed the
  completed transcript and reran the requested command.

Claude Code's documented CLI supports caller-selected UUIDs and specific
session resume; its session documentation also says session state is local to
the working directory and that simultaneous non-forked resumes can interleave.
The experiment kept the same isolated working directory across both attempts
and never used `--fork-session` or `--continue`.
See the [CLI reference](https://code.claude.com/docs/en/cli-usage) and
[session documentation](https://code.claude.com/docs/en/sessions).

## Evidence and admission

The admitted v5 root contains 12 runs, 21 successful terminal provider streams,
21 immutable process-start receipts, and 345 raw artifacts. Each receipt records
the launched binary, complete argument vector, work directory, Activity attempt,
Worker actor, PID, and process-start identity. Admission independently requires
exactly one `--session-id <selected-uuid>` pair on attempt 1 or exactly one
`--resume <selected-uuid>` pair on later attempts, and rejects alternate,
duplicate, conflicting, fork, or continue controls.

An independent post-run audit matched all selected UUIDs against every provider
event, all destination and workspace effects, all common-manifest hashes, and
all 345 per-run inventory entries. Every captured Workflow history replayed
against the source-matched Workflow. The provider streams recorded 63 turns and
`$0.1800649` in aggregate provider-reported cost across the 21 invocations; this
is an episode accounting observation, not a cost comparison.

The Git-safe transport under
[`resume-evidence-transport`](../../experiments/durable-vendor-sessions/claude-direct/resume-evidence-transport/transport-index.json)
binds the full v1-v5 correction lineage: 1,759 files, 8,166,362 uncompressed
bytes, and 44 finalized run inventory/verdict chains. Its index SHA-256 is
`107da44f12f0e9c9b6bd0a76095790e2943dd655c141d74d48ebfff779f838d3`.

The non-admitted roots remain material observations:

- v1 preserved one clean run and a same-session duplicate-effect episode whose
  retry exhausted a two-turn ceiling before producing structured output;
- v2 completed but lacked raw invocation arguments;
- v3 completed before admission rejected every conflicting flag encoding; and
- v4 preserved seven finalized runs plus a partial episode where a two-second
  heartbeat window expired during contended-host process setup before the first
  session receipt. V5 starts heartbeating at Activity entry and uses a
  15-second detection margin.

No root was deleted or rewritten.

## Responsibility split

- Temporal durably records the caller-selected UUID in Workflow input, detects
  Worker loss, redelivers the Activity, and accepts one completion. It does not
  attach to the surviving CLI process or fence a second prompt in that session.
- The application maps one stable Activity ID to attempt 1 start and later
  resume commands. In this arm it deliberately supplies no current-owner
  generation, start-or-attach registry, stale-completion gate, or effect
  reconciliation.
- Claude Code accepts the selected UUID and reports it on both invocations.
  That observation establishes session-identity reuse, not one live executor or
  one command execution.
- The BoltDB destination and fixture journal establish physical effect count.
  A production destination must still deduplicate, fence, or reconcile its own
  mutation protocol.

## Scope and what would change this conclusion

The trials used Claude Haiku through one authenticated account, a local Git
fixture, and local BoltDB on one contended Linux host. They killed the Worker,
not the host or Temporal Service. They do not establish cross-host transcript
portability, cancellation, version compatibility, interactive PTY attachment,
or full transcript equivalence beyond the selected ID and preserved streams.
Three trials per boundary establish repeatability, not a production frequency.

The claim is falsified if fresh source-pinned trials using caller-selected
session resume but no process attachment, ownership fence, destination
idempotency, or reconciliation retain one physical effect at all three Worker
loss boundaries; if the raw invocation does not prove the declared start/resume
operation; if selected and observed session IDs differ; if a named barrier or
process identity is wrong; or if the accepted outcome disagrees with the
independent workspace and destination evidence.
