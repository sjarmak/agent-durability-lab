# Durable vendor coding-agent sessions

**Status:** active experiment; unsafe direct-Claude arm complete, 2026-08-08
**Tracking:** `temporal_projects-5im`
**Vendors in the first study:** Claude Code and Codex

## Decision

The lab will test whether Temporal can durably supervise a logical coding-agent
session implemented by a vendor product. The experiment will not treat a
resumable vendor transcript as proof that the executing turn, workspace, or
external effects are durable.

The first implementation will use headless, structured interfaces. Interactive
PTY attachment is a later arm because terminal bytes hide the turn and tool
boundaries needed for exact fault injection and an independent oracle.

This is an experiment plan, not a supported guarantee or a commitment to ship a
Temporal integration.

## Question

Can a Temporal application recover a Claude Code or Codex session after Worker,
runner, vendor-process, or host failure while preserving all of the following?

- at most one authorized owner advances a logical turn;
- at most one turn result is accepted;
- stale attempts cannot publish tools, effects, or completion;
- cancellation removes authority before best-effort process termination;
- the accepted result agrees with the independently observed workspace; and
- ambiguous effects remain explicit rather than being converted into success.

The product question is narrower: does a reusable external-agent session
protocol remove failure-handling work that users would otherwise implement, or
does it merely wrap vendor CLI invocation in an Activity?

## Existing capabilities and unresolved gaps

The distinctions below prevent several different meanings of "durable session"
from collapsing into one claim.

| Layer | Vendor capability to verify | Temporal or application responsibility under test |
| --- | --- | --- |
| Transcript | Persist a conversation and resume by vendor session or thread ID | Persist the logical-session mapping and its compatibility metadata |
| Turn and process | Stream turn events, interrupt work, and possibly reconnect to a live turn | Serialize turns, start or attach after retry, fence ownership, and route cancellation |
| Sandbox resource | Provision, attach, suspend, resume, snapshot, fork, and stop isolated compute | Preserve resource identity, lifecycle ownership, cleanup receipts, restore lineage, and provider-specific semantics |
| Workspace and effects | Execute file edits, commands, commits, and remote tools | Isolate writers, observe destination state, deduplicate or reconcile effects, and reject stale authority |

### Claude Code surface

Locally re-verified with Claude Code 2.1.226 on 2026-08-08 (the earlier plan
inspection used 2.1.224 on 2026-08-07):

- `claude -p` runs a non-interactive turn;
- `--output-format stream-json` exposes structured output;
- `--session-id <uuid>` lets the caller choose the vendor session identity before
  process launch; and
- `--resume <id>` continues a saved session.

Anthropic documents continuous local transcript persistence, headless session
resumption, Agent SDK resumption after process restart, and external
`SessionStore` implementations for environments that cannot retain local session
files. Anthropic also documents that concurrent resumes can interleave messages
in one transcript. That behavior makes transcript persistence an unsafe
substitute for single-writer ownership.

Primary references:

- [Claude Code CLI reference](https://code.claude.com/docs/en/cli-reference)
- [Run Claude Code programmatically](https://code.claude.com/docs/en/headless)
- [Manage sessions](https://code.claude.com/docs/en/sessions)
- [Agent SDK sessions](https://code.claude.com/docs/en/agent-sdk/sessions)
- [Persist sessions in external storage](https://code.claude.com/docs/en/agent-sdk/session-storage)

### Codex surface

Locally observed with Codex CLI 0.147.0 on 2026-08-07:

- `codex exec` is the non-interactive command; `-p` selects a configuration
  profile and is not the headless-mode flag;
- `codex exec --json` emits JSONL events;
- `codex exec resume <session-id>` resumes persisted state; and
- the experimental App Server exposes explicit thread, turn, interruption,
  approval, process, and event-stream operations.

The locally generated App Server schema says `thread/resume` can rejoin a thread
already running in that server. Unlike Claude Code's `--session-id`, the observed
Codex start interfaces do not let the caller choose the vendor thread ID before
launch. The resulting process-start/thread-registration gap is a first-class
failure boundary, not an implementation inconvenience.

Primary references:

- [Codex CLI reference](https://developers.openai.com/codex/cli/reference)
- [Codex non-interactive mode](https://developers.openai.com/codex/noninteractive)
- [Codex App Server](https://developers.openai.com/codex/app-server)

These vendor observations are experiment inputs. They do not establish the
application invariants above.

### Temporal Durable Agentic Harness prior art

The supplied [Durable Agentic Harness](https://temporal.io/code-exchange/durable-agentic-harness)
is a separate, Temporal-reviewed Code Exchange project backed by
`temporal-sa/durable-agentic-harness`. Source review on 2026-08-07 pinned commit
[`4afef65`](https://github.com/temporal-sa/durable-agentic-harness/tree/4afef65defcd8e70d6e794936320e4d7513fd365).
It is a stock-trading demonstration, not the Sandbox Orchestration Harness.

Its strongest reusable pattern is a Temporal-native agent loop:

- the Python `OpenAIAgentsPlugin` lets Workflow code call the OpenAI Agents SDK
  while dispatching model calls as Temporal Activities;
- `activity_as_tool` makes selected agent tools explicit Activities in Event
  History;
- child Workflows give parallel backtests stable Temporal identities;
- Signals and Queries implement human approval, steering, stop, and inspection;
- structured `TradeIntent` output creates a typed boundary between model output
  and deterministic application policy; and
- the Workflow replaces the model's repeated intent ID with a stable
  Workflow-and-tick ID, then passes a derived idempotency key to the broker.

This is the missing control group for the vendor-session study. We should compare
Claude Code and Codex with a small Temporal-native agent loop that uses the same
fixture, tool, fault schedule, and oracle. That comparison can show what becomes
observable and recoverable when individual model and tool turns are Temporal
operations, versus when the whole session remains owned by an external product.
We should extract the integration pattern, not reproduce the trading application
or its frontend.

The demonstration does not establish the external-session guarantees in this
plan:

- its documented chaos path kills a Worker during an LLM Activity and shows
  Workflow replay; it does not test a surviving external CLI, concurrent resume,
  stale authority, or post-effect/pre-completion ambiguity;
- the broker Activity supplies a stable destination idempotency key, confirming
  that effect safety comes from the application and broker rather than Activity
  retry alone;
- strategy persistence, trade-record persistence, and UI notification are
  retryable Activities without the same explicit effect identity;
- the backtest Activity creates an unnamed Docker container and relies on a
  `finally` block for removal. Worker death after container creation can bypass
  that cleanup and a retry can create another container;
- API start generates a fresh Workflow ID for each request while the included
  SQLite idempotency table is not used by that route, so client-response loss can
  start two logical agents; and
- the live SSE bus is process memory, while notification is a retryable Activity;
  Event History durability does not make subscriber delivery durable or
  duplicate-free.

The main agent Workflow is an unbounded tick loop with no demonstrated
Continue-As-New boundary. The vendor-session experiment should therefore include
history growth and continuation while preserving session, sandbox, approval,
cursor, and ownership identities.

Primary references:

- [Durable Agentic Harness Code Exchange entry](https://temporal.io/code-exchange/durable-agentic-harness)
- [Durable Agentic Harness source](https://github.com/temporal-sa/durable-agentic-harness)
- [OpenAI Agents plugin configuration](https://github.com/temporal-sa/durable-agentic-harness/blob/4afef65defcd8e70d6e794936320e4d7513fd365/backend/worker/main.py#L46-L93)
- [Workflow-native agent loop](https://github.com/temporal-sa/durable-agentic-harness/blob/4afef65defcd8e70d6e794936320e4d7513fd365/backend/worker/workflows/parent.py#L283-L322)
- [Workflow-minted broker idempotency key](https://github.com/temporal-sa/durable-agentic-harness/blob/4afef65defcd8e70d6e794936320e4d7513fd365/backend/worker/workflows/parent.py#L271-L281)

### Temporal Sandbox Orchestration Harness prior art

Source review on 2026-08-07 covered the May 2026 blog and Code Exchange entry
plus `temporal-community/sandbox-orchestration-harness` at commit
[`e8a8854`](https://github.com/temporal-community/sandbox-orchestration-harness/tree/e8a88540d9523a3d9070860913567670194bacc1).

The sample adds a useful durable resource model that this plan previously left
implicit:

- each sandbox has a stable UUID and a dedicated child `SandboxWorkflow`;
- commands and lifecycle operations are sent as Workflow Updates with stable
  Update IDs;
- an owning handle can wait for shutdown, while an opaque reference lets another
  Workflow route commands without taking lifecycle ownership;
- lifecycle state distinguishes pending, running, suspended, failed, and deleted;
- cleanup policy is explicit rather than hidden in process teardown;
- snapshots can restore or fork workspace state; and
- provider capability differences are declared instead of pretending every
  backend implements suspend, resume, snapshot, and fork identically.

We should reuse those shapes. The vendor agent session and its sandbox should be
modeled as separate durable resources with separate identities and lifecycles.
A session may survive while its sandbox is replaced from a snapshot; a sandbox
may survive while the vendor turn is lost. Owned versus attached handles, stable
request IDs, explicit cleanup policy, provider capability declarations, and
snapshot lineage all belong in the experiment and any eventual connector API.

The implementation also exposes boundaries that the examples do not establish
as safe:

- the stable Update ID deduplicates delivery into `SandboxWorkflow`, but
  `ExecuteCommand` then runs as a retryable Activity whose provider interface
  receives only sandbox status and a command string, not a stable command
  operation ID;
- `StartSandbox` is also a retryable provider Activity. Several reviewed
  providers create a new remote resource and learn its provider instance ID only
  from the response, leaving a create-success/Activity-completion ambiguity;
- an opaque sandbox reference contains routing identity, not an owner generation
  or revocable capability. Shared access is intentional, so single-writer
  authority must be added above or enforced by the destination;
- the sample does not add an application ownership fence or a demonstrated
  serialization rule for commands arriving through multiple attached handles;
- cleanup after parent closure relies on the Workflow having received provider
  status. A sandbox created before a lost `StartSandbox` completion may not be
  named for cleanup; and
- snapshot persistence does not by itself identify which commands, vendor
  transcript events, or external effects the snapshot incorporates.

The repository contained examples but no Go test files at the reviewed commit.
The blog's “No orphans” and durable-tool-call language therefore remain design
hypotheses for this lab, not evidence. The harness will be a pinned prior-art and
calibration arm, not an assumed production dependency.

Primary references:

- [Temporal Sandbox Orchestration Harness blog](https://temporal.io/blog/temporal-sandbox-orchestration-harness-the-missing-layer-for-running-agents)
- [Temporal Code Exchange entry](https://temporal.io/code-exchange/temporal-sandbox-orchestration-harness)
- [Sandbox Orchestration Harness source](https://github.com/temporal-community/sandbox-orchestration-harness)
- [Stable Update ID forwarding](https://github.com/temporal-community/sandbox-orchestration-harness/blob/e8a88540d9523a3d9070860913567670194bacc1/sdk/sandbox_activity.go#L56-L77)
- [Provider command Activity](https://github.com/temporal-community/sandbox-orchestration-harness/blob/e8a88540d9523a3d9070860913567670194bacc1/sdk/workflow/activities.go#L82-L102)

## Experiment contract

### Hypotheses

1. A retryable Activity that directly invokes a vendor CLI can create a second
   writer or duplicate a completed tool effect when the first process survives
   or its completion is lost.
2. Persisting and resuming the vendor session ID reduces transcript loss but does
   not, by itself, prevent concurrent turns, stale completion, or duplicate
   workspace effects.
3. A stable logical session and turn ID plus an application-owned generation and
   start-or-attach supervisor can prevent competing accepted owners within the
   enforcement boundaries that check the generation.
4. Destination effects remain only as strong as the destination's idempotency,
   transaction, fencing, or reconciliation protocol.
5. A stable Workflow Update ID can deduplicate the control-plane request while a
   retried inner provider Activity still duplicates sandbox creation or command
   effects.
6. Snapshot and fork can make workspace lineage recoverable, but only when the
   snapshot is bound to a recorded transcript cursor, accepted command prefix,
   and destination-effect state.
7. A Temporal-native agent loop makes model and tool turns individually visible
   and recoverable, but client start, destination effects, event delivery, and
   sandbox processes still require their own identities and recovery protocols.

### Invariant

For one admitted logical turn, at most one current ownership generation may
advance the turn or publish its accepted result. The final Workflow result must
agree with the independently observed repository and destination state.

Cancellation adds a second invariant: after durable authority revocation, no
later mutation or completion from the revoked generation may be accepted.

### Responsibility split

```text
Temporal Workflow
  logical session, ordered turns, retries, waits, approvals, cancellation
        |
        v
agent-session supervisor
  start-or-attach, generation fence, process control, event buffering
        |
        +-- Claude Code stream-json or Agent SDK
        |
        +-- Codex App Server or codex exec fallback
        |
        v
sandbox lifecycle workflow
  provision, attach, suspend, snapshot/fork, restore, cleanup
        |
        v
isolated compute and worktree
        |
        v
independently observed effect destinations
```

Temporal supplies durable procedure and redelivery. The application supplies the
stable logical identity, current owner generation or capability, session
registry, attachment policy, and outcome acceptance. The runner owns the actual
vendor process and its process tree. The workspace and every remote destination
remain separate state machines whose acceptance rules determine effect safety.
The sandbox lifecycle is another state machine: its stable Workflow and Update
identities do not automatically deduplicate provider creation or command effects.

The proposed supervisor operation is:

```text
StartOrAttachTurn(
    logical_session_id,
    ownership_generation,
    logical_turn_id,
    prompt
) -> existing_or_new_turn_receipt
```

Reissuing the same current turn attaches to, observes, or returns its existing
receipt. It must not silently launch a competitor. A stale generation must fail
before tool execution and again before result publication.

### Experimental arms

| Arm | Mechanism | Purpose |
| --- | --- | --- |
| Unsafe control | A retryable Activity starts a fresh CLI process with no stable turn ownership | Demonstrate that the oracle detects duplicate writers, effects, or accepted results |
| Vendor resume only | Persist the vendor session or thread ID and use the vendor resume operation after retry | Isolate what transcript/session persistence provides and what it does not |
| Fenced supervisor | Use stable logical IDs, a monotonic generation or opaque capability, and start-or-attach | Test the smallest credible external-session ownership protocol |
| Protocol-native follow-up | Claude Agent SDK plus external `SessionStore`; Codex App Server | Determine whether structured live-session APIs improve recovery over one-process-per-turn CLI invocation |
| Sandbox-harness calibration | Pin the community harness and begin with a hermetic provider implementation | Separate durable sandbox lifecycle and Update deduplication from provider operation and workspace-effect safety |
| Temporal-native baseline | Use the Python OpenAI Agents SDK integration with one model turn and one controlled Activity tool | Compare Temporal-visible model/tool turns with an externally owned Claude Code or Codex session under the same faults |

Claude Code will run first because a caller-selected session UUID can be
persisted before launch. Codex runs next because its learned-after-start thread
identity exercises the harder registration ambiguity. `codex exec` provides a
matched CLI arm; App Server is the preferred protocol-native arm.

### Exact failure boundaries

The controller must wait for named evidence, inject the fault, and only then
release the blocked operation. `time.Sleep` is not an admissible barrier.

| Boundary | Fault or race | Question answered |
| --- | --- | --- |
| Client start accepted / start response returned | Lose the API response and repeat the request | Does a stable application request and Workflow ID return the existing logical session, or start a second agent? |
| Process created / vendor ID registered | Kill the Worker or runner after process creation but before durable vendor-ID registration | Can recovery discover the original work, and what happens when the ID is not caller-selected? |
| Provider sandbox created / provider status recorded | Lose the `StartSandbox` Activity completion | Does retry attach by a stable provider operation ID, reconcile a created instance, or orphan and duplicate compute? |
| Vendor ID registered / first tool request | Kill after registration | Does resume continue the intended lineage without creating another active turn? |
| Tool effect committed / tool result recorded | Kill after a controlled file, command, Git, or API effect succeeds | Does retry duplicate, reconcile, or explicitly report ambiguity? |
| Native model or tool call / Activity completion | Kill during and after the Temporal-native baseline operation | Which prefix is recovered from Event History, what is physically repeated, and which costs or effects need separate deduplication? |
| Sandbox command effect / command Activity completion | Lose the provider command response after the process changes the workspace | Does a stable outer Update ID prevent an inner Activity retry from running the command again? |
| Final vendor result emitted / Activity completion recorded | Kill after final structured output | Can one physical turn produce two accepted logical completions or unnecessary repeated work? |
| Original turn alive / retry begins | Delay or partition the original runner while a new attempt starts | Does recovery attach, fence a replacement, or create competing writers? |
| One sandbox reference / concurrent attached commands | Submit the same or conflicting logical turn through owning and attached handles | Are commands serialized by current ownership, intentionally forked, or allowed to compete in one workspace? |
| Authority revoked / process exits | Freeze the vendor process during cancellation | Can stale work mutate after Temporal records cancellation? |
| Parent closed / provider cleanup acknowledged | Close during initialization, a command, suspension, and normal idle state | Can cleanup identify every created provider instance and distinguish requested, delivered, acknowledged, and verified deletion? |
| Graceful stop / Workflow cancel or terminate | Exercise each closure path with live child work and provider resources | Which paths run application revocation and cleanup, and which immediately end durable procedure without it? |
| Host-local state lost / session resumed elsewhere | Stop the runner host and resume from shared session storage | Which transcript, process, and workspace state is actually portable? |
| Snapshot created / snapshot reference recorded | Lose completion after provider snapshot creation | Is the snapshot leaked, duplicated, or recoverable by a stable operation identity? |
| Snapshot prefix / restored execution | Restore and continue from a recorded snapshot | Which transcript events, commands, artifacts, credentials, and external effects are included or intentionally excluded? |
| Version N / version N+1 | Change Worker, runner, or vendor CLI version between turns | Does attachment fail closed when protocol or session compatibility is unknown? |
| One resume / concurrent resume | Start two resumes for the same session and turn | Does the vendor serialize, reject, rejoin, fork, or interleave? |
| Stream prefix / retry reconstruction | Interrupt after an acknowledged event prefix | Which events are duplicated, lost, reordered, or reconstructable? |
| History continuation / new Workflow Run | Continue a long session with an outstanding sandbox, transcript cursor, and approval | Which identities and ownership checks survive Continue-As-New, and which derived state must be rebuilt? |

### Evidence and oracle

Each raw event must retain:

- logical session and turn IDs;
- client start request ID and returned Workflow/run identity;
- vendor and vendor session or thread ID;
- ownership generation and capability hash;
- Workflow ID, Run ID, Activity ID, and Temporal attempt;
- Worker, runner, host, process ID, and process-start identity;
- CLI or SDK version, model, configuration, sandbox, and tool-permission hashes;
- repository revision, worktree identity, and before/after tree hashes;
- sandbox Workflow and Update IDs, provider type, provider instance identity,
  lifecycle owner, cleanup policy, and cleanup disposition;
- snapshot identity, parent snapshot, workspace hash, transcript cursor, accepted
  command prefix, and fork lineage;
- vendor event sequence and transcript cursor or digest;
- native agent integration, model-call, tool-call, approval, and continuation
  identities where the Temporal-native baseline is used;
- logical effect ID and physical command, edit, commit, artifact, or API receipt;
- barrier arrival, injected fault, cancellation, revocation, completion, and UTC
  timestamps.

The independent oracle must decide at least:

- number of authorized generations and concurrent same-worktree writers;
- number of physical executions per logical tool effect;
- number and identity of accepted turn results;
- whether stale generations attempted or completed mutations;
- whether any mutation occurred after revocation;
- whether the Workflow result matches the final workspace and destination;
- whether transcript lineage is continuous, explicitly forked, or unresolved;
- whether every observed provider instance has a current owner or a verified
  terminal cleanup disposition; and
- whether a restored snapshot corresponds to the declared workspace and command
  prefix rather than merely containing plausible files.

Vendor self-reports and a successful Workflow completion are observations, not
acceptance evidence.

### Trial admission

A clean no-fault trial establishes the fixture. The unsafe arm must violate an
invariant under at least one declared fault or the experiment cannot distinguish
the proposed mechanism. Each concurrency-sensitive arm needs at least three
independent live trials on pinned versions.

A trial is invalid rather than passing when the named barrier was missed, the
wrong process was targeted, required identities are absent, or the independent
workspace/destination observation cannot be completed. Failed and invalid raw
evidence remains preserved.

### Falsifiers

The protected claim becomes false or narrower if any of the following occurs:

- two current or stale generations both produce accepted mutations;
- a retry launches a competitor when exact attachment evidence existed;
- resume changes transcript lineage without an explicit fork;
- the Workflow accepts a result inconsistent with repository state;
- a canceled generation mutates after authority revocation;
- recovery reports success when effect state is unknowable;
- stable Workflow Update delivery is reported as exactly-once provider command
  execution;
- parent closure reports successful cleanup while an unregistered provider
  instance remains live;
- two attached handles mutate one workspace without a declared ownership or fork
  policy;
- a retried client start creates a second logical agent for the same request;
- Workflow replay is reported as recovery of an external process or as
  deduplication of a model, tool, notification, or destination effect;
- cancellation or termination reports cleanup that its closure path did not run;
- Continue-As-New loses or silently changes session, sandbox, approval, cursor,
  or ownership identity;
- a restored snapshot cannot be bound to the recorded transcript and command
  lineage; or
- correctness depends on a host-local transcript, PID, timing guess, or
  unrecorded vendor behavior.

## Build sequence

The prerequisite common evidence boundary is complete. Its source-pinned live
suite exercises the shared simulator, authority/effect state, named barriers,
process controller, append-only writer, and independent oracle; see
[Finding 0007](../findings/0007-live-common-harness-calibrates-the-oracle.md).
Vendor and Temporal-native arms must consume that boundary rather than create
their own verdict or evidence format.

1. Build a deterministic fixture repository and controlled tool destination.
   The task must expose barriers around one observable edit or effect and a final
   result; it must not depend on model judgment for the oracle.
2. Build the smallest Temporal-native OpenAI Agents integration baseline: one
   structured model result, one controlled Activity tool, one Workflow-minted
   logical effect ID, and an optional approval Signal. Use the same fixture and
   oracle as the vendor arms; do not port the trading demo. Track this as
   `temporal_projects-5im.4`.
3. Calibrate the Sandbox Orchestration Harness against a hermetic provider.
   Reproduce provider-create and command-effect completion loss, attached-handle
   concurrency, parent-close cleanup, and snapshot-reference loss before using a
   paid or remote sandbox backend. Track this as `temporal_projects-5im.2`.
   **Completed:** 36 admitted live trials at pinned commit `e8a8854` separated
   outer Update deduplication from inner provider idempotency, rejected stale
   attached writers with provider fencing, restored exact snapshot prefixes,
   and showed that parent-close cleanup needs provider reconciliation when
   status was never accepted. See [Finding 0009](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).
4. Implement the unsafe direct-subprocess arm for Claude Code and prove the
   negative control fails. **Complete:** `temporal_projects-5im.5` produced 12
   admitted authenticated Claude Code 2.1.226 trials at the three declared
   direct-CLI boundaries. Three unfaulted trials passed. All nine Worker-loss
   trials launched two distinct Claude sessions, applied the stable logical
   effect twice, and still produced one accepted Temporal outcome. Exact
   barriers, independent workspace and destination evidence, raw provider
   streams, and manifest-bound inventories distinguish the observed vendor
   result from the hermetic calibration. See [Finding 0010](../findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md)
   and the [unsafe direct-Claude control](../../experiments/durable-vendor-sessions/claude-direct/README.md).
5. Add caller-selected Claude session identity and the resume-only arm.
   **Complete:** `temporal_projects-5im.7` produced 12 admitted authenticated
   Claude Code 2.1.226 trials. Every Worker-loss retry resumed the selected UUID
   and still duplicated the physical effect; transcript identity was not an
   ownership or effect fence. See [Finding 0019](../findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md).
6. Add the fenced start-or-attach supervisor and cancellation revocation.
   **Complete:** `temporal_projects-5im.8` produced matched hermetic and
   authenticated Claude Code `2.1.227` comparisons. In each comparison the
   resume-only control failed all nine Worker-loss trials with duplicate
   effects, while the protected arm passed 15/15 runs across four exact
   boundaries with one process/effect/outcome per logical run and 12 exact
   recovery attachments. All 54 histories replayed, every sealed population
   passed independent disk audit, and deterministic transports preserve the
   rejected logged-out attempt and admitted fixed-profile evidence. See
   [Finding 0020](../findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md).
7. Repeat the matched CLI experiment with `codex exec`, including the
   pre-registration gap.
8. Add Codex App Server and Claude Agent SDK/`SessionStore` protocol-native arms
   only where they distinguish a recovery guarantee.
   Evaluate Claude Code 2.1.226's background-session daemon (`--bg`, `attach`,
   `respawn`, `stop`, and daemon restart/keep-workers) separately under
   `temporal_projects-5im.6`; do not conflate it with transcript resume.
9. Test true interactive PTY attachment only after structured headless recovery
   has a supported invariant and oracle.
10. Add a Continue-As-New boundary only after the single-run identity and
    evidence contract is stable.
11. Update [the guarantee ledger](../guarantees.md) and add a finding only after
   live evidence supports a bounded claim.

The one-off mechanisms should remain beside the experiment until a second vendor
or scenario demonstrates a shared boundary worth extracting.

## Product decision gates

Evidence would support a reusable Temporal integration when both vendors need
the same logical contract and the fenced supervisor materially improves the
oracle over vendor resume alone. The Temporal-native baseline must also show
which complexity disappears when model and tool turns are first-class Temporal
operations; that delta defines the minimum external-session bridge worth
productizing. A candidate product surface would include:

- a vendor-neutral external-agent session and turn identity model;
- start-or-attach, explicit fork, interrupt, and current-owner completion;
- durable structured events with cursors and deduplication identities;
- approval and cancellation routing;
- workspace and artifact references rather than large history payloads;
- connector capability declarations for caller-selected IDs, live attachment,
  transcript portability, interrupt behavior, version compatibility, sandbox
  suspension, snapshot, fork, and cleanup behavior; and
- a fault-conformance suite that vendors and integrations can run.

Agent-session ownership and sandbox-resource ownership should remain composable
contracts rather than one large “agent runtime” abstraction. The former governs
conversation and turn authority; the latter governs isolated compute and
workspace lineage. A product should let users adopt either without inventing a
false atomic boundary between them.

The integration belongs in an SDK or contrib package before it belongs in the
Temporal Server. A server feature is justified only if the experiments reveal a
cross-language state transition or enforcement boundary that SDK/application
code cannot implement consistently.

Evidence would argue against productization when the useful logic remains
vendor-specific process supervision, when resume-only performs equivalently, or
when correctness depends primarily on a destination protocol Temporal cannot
mediate. In that case, the useful output is integration guidance and a test
suite rather than a new abstraction.

## Explicit non-claims

This plan does not claim that:

- a vendor session ID is a durable logical owner;
- Workflow replay of a native agent loop proves recovery of a vendor-owned
  process or session;
- a sandbox Workflow ID or attachable reference is a revocable owner capability;
- resuming a transcript resumes a live process or partially completed turn;
- a stable Workflow Update ID makes the inner provider command effect exactly
  once;
- successful parent-Workflow closure proves every provider resource was cleaned
  up;
- a filesystem snapshot captures vendor transcript or remote-effect state;
- Activity retry makes file, shell, Git, artifact, or API effects exactly once;
- process termination proves destination authority was revoked;
- App Server or Agent SDK preview behavior is a stable compatibility contract;
- a single-host result establishes cross-host recovery; or
- placing a CLI inside an Activity makes an interactive agent application
  correct after recovery.

The hermetic and authenticated Claude Code `2.1.227` results support the bounded
claim: Temporal can durably redeliver the procedure while an application
supervisor supplies stable identity, current-owner fencing, attachment, and
destination reconciliation on the tested single host. Supervisor/host loss,
cross-host routing, deployment/version change, and destination-general
enforcement remain unresolved.

## Related research

- [Research questions](../research-questions.md)
- [Laboratory architecture](../architecture.md)
- [Experiment methodology](../experiment-methodology.md)
- [Current Temporal surface](../current-temporal-surface.md)
- [Finding 0001: Worker death with a surviving agent](../findings/0001-worker-death-surviving-agent.md)
- [Finding 0002: Launch decision is not process liveness](../findings/0002-launch-decision-is-not-process-liveness.md)
- [Finding 0004: One Temporal completion can hide two effects](../findings/0004-one-temporal-completion-can-hide-two-effects.md)
- [Finding 0006: Cancellation requires application revocation](../findings/0006-cancellation-requires-application-revocation.md)
- [Finding 0007: Live common harness calibration](../findings/0007-live-common-harness-calibrates-the-oracle.md)
- [Finding 0020: Application-fenced Claude supervisor](../findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md)

Related open work includes agent-session compatibility across Worker Versioning
(`temporal_projects-0xm`), partial Workflow Stream recovery
(`temporal_projects-cg5`), destination-enforced revocation
(`temporal_projects-k4x`), and recurring-fault degradation
(`temporal_projects-uju`).
