# External CLI ownership

**Maturity: Normative for the tested single-host external-CLI ownership
contract at pinned Claude Code `2.1.227` and Codex CLI `0.147.0` versions.**
Broad vendor and version compatibility remains unclaimed. The negative controls
come from [Finding 0010](../../../docs/findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md)
and [Finding 0019](../../../docs/findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md);
the matched protected observations are [Finding 0020](../../../docs/findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md)
and [Finding 0021](../../../docs/findings/0021-codex-thread-resume-is-not-turn-authority.md).

## Question

How can a Temporal Activity recover an external coding-agent CLI after Worker
loss without direct relaunch starting a competing turn, resume-only preserving
a transcript while duplicating effects, or a stale executor publishing the
accepted result?

## Invariant

For one stable session, turn, operation, and effect, exactly one generation and
opaque capability is current. Only that authority may register a process,
apply a destination mutation, publish a result, or complete the turn. Activity
attempt, PID, and provider session/thread are observations, not ownership.

A redelivery asks an application-owned supervisor to start-or-attach. A live
execution is addressed by exact PID/start identity, process group, provider
session/thread, and result channel. Replacement commits generation N+1 before
launch: this is the fence-before-replace rule. N remains stale even after an
ABA-shaped request. Cancellation commits
revocation before signaling and is acknowledged only after the exact process
group is verified empty.

## Failure boundary

The evidence separates three designs:

- **direct relaunch:** redelivery starts a fresh CLI process;
- **resume-only:** redelivery reuses the provider session/thread learned or
  selected on attempt 1, but two processes can still cross the effect boundary;
- **start-or-attach:** an external supervisor owns process lifetime and
  authority, so redelivery attaches or performs a fenced replacement.

Both vendors test claim-before-exec, process-before-provider-registration,
tool-effect-before-completion, and final-output-before-completion. The Codex
protected arm also tests thread observation before durable registration,
concurrent recovery at the effect barrier, authorized process failure before a
thread exists, and cancellation while executing. Every concurrency-sensitive
boundary repeats three times with a named barrier rather than a timing guess.

## Oracle

`run.sh check` verifies and restores the clone-safe packages, recomputes the
Claude verdicts, then independently audits and replays all six current Codex
transports:

| Provider and arm | Admitted result |
| --- | --- |
| Claude direct | 12 runs: 3 pass, 9 duplicate-effect fail |
| Claude resume-only | 12 runs: 3 pass, 9 duplicate-effect fail despite one selected UUID |
| Claude fenced | 15/15 pass, 12 exact attachments, one process/effect/outcome per run |
| Codex unsafe fresh | 12 runs: 6 pass, 6 duplicate-effect fail |
| Codex explicit resume | 12 runs: 6 pass, 6 duplicate-effect fail despite one logical thread |
| Codex fenced | 27/27 pass, 21 attachments, 3 replacements, 3 cancellations |

The accepted result must agree with authority state, exact process provenance,
raw provider stream, destination and workspace state, and Temporal history
replay. Missing or contradictory evidence is invalid, not a pass. The Codex
comparison contributes 102 replayed histories across matched hermetic and
authenticated populations.

`run.sh critical` executes the source-pinned Claude resume-only control,
protected supervisor path, and repeated concurrency mechanisms. It is a
mechanism gate, not a provider-performance benchmark.

## Fresh-checkout run

With Go and the pinned Temporal CLI available, run:

```bash
./cookbooks/coding-agents/03-external-cli-ownership/run.sh all
```

The audit and critical path are credential-free. Generating a fresh
authenticated population requires an already configured fixed provider
profile and a new append-only evidence root; never point generation at a
preserved root.

## Evidence

Claude evidence is preserved in the direct
[`evidence-transport`](../../../experiments/durable-vendor-sessions/claude-direct/evidence-transport/transport-index.json),
resume-only
[`resume-evidence-transport`](../../../experiments/durable-vendor-sessions/claude-direct/resume-evidence-transport/transport-index.json),
hermetic protected
[`fenced-evidence-transport-v2`](../../../experiments/durable-vendor-sessions/claude-direct/fenced-evidence-transport-v2/transport-index.json),
and authenticated protected
[`fenced-claude4-evidence-transport-v3`](../../../experiments/durable-vendor-sessions/claude-direct/fenced-claude4-evidence-transport-v3/transport-index.json)
packages.

Codex v4 transports preserve current v12 populations plus relevant rejected or
superseded lineage:

- [hermetic unsafe](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/hermetic-unsafe-20260812-v4/transport-index.json),
  [resume](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/hermetic-resume-20260812-v4/transport-index.json), and
  [fenced](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/hermetic-fenced-20260812-v4/transport-index.json);
- [authenticated unsafe](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/auth-unsafe-20260812-v4/transport-index.json),
  [resume](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/auth-resume-20260812-v4/transport-index.json), and
  [fenced](../../../experiments/durable-vendor-sessions/codex-direct/evidence-transport/auth-fenced-20260812-v4/transport-index.json).

The check path consumes these tracked transports rather than local raw roots,
so it is clone-safe and does not require provider credentials.

## Observed result

For authenticated Claude, direct retry used two provider sessions and effects
in every faulted run. Resume-only retained one caller-selected UUID but still used two
processes and effects. The protected arm passed 15/15 with 12 attachments and
one authoritative process/effect/outcome per run.

For Codex, unsafe fresh and explicit thread resume each produced six passes and
six duplicate-effect failures in both hermetic and authenticated populations.
The protected arm passed 27/27 in both environments, including attachment,
concurrent recovery, replacement before thread observation, and cancellation.
Each protected population recorded 30 processes, 27 threads, 24 effects, 21
attachments, three replacements, three cancellations, zero capability leaks,
and 27 replayed histories.

The shared conclusion is narrower than vendor support: provider transcript
identity is useful observation, but application-owned authority and a
destination-enforced fence are what make the tested recovery safe. This is not exactly once.

## Responsibility split

- Temporal records and replays procedure, detects Worker loss, and redelivers
  the Activity. It does not discover or fence the CLI.
- The application supervisor owns stable logical identity, generation and
  capability allocation, process/session/thread registration, start-or-attach,
  replacement, cancellation revocation, and conditional publication.
- The authority store serializes ownership transitions; each mutating
  destination enforces current generation/capability and stable effect identity.
- The provider supplies its session/thread identity and stream. Those remain
  observations, not owner capabilities.

## Falsifier

The contract is falsified if a source-matched protected run starts two current
executors, accepts a stale or canceled effect/result/completion, replaces
before the newer fence commits, misses an exact declared attachment, records
wrong command/process/session provenance, disagrees with destination/workspace
state, leaks a capability, or fails replay. Provider-specific compatibility is
also falsified if fresh runs at a recorded version and fixed profile do not
reproduce the result.

The contract does not cover authentication durability, cross-host supervisor
failover, arbitrary destinations, authority-store disaster recovery, provider
version migration, interactive or protocol-native APIs, semantic transcript
equivalence, performance/failure rates, or an exactly-once guarantee.
