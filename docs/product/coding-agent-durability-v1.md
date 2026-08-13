# Coding-agent durability v1

**Status:** internal product specification; not published or a supported-product
commitment

**Evidence snapshot:** 2026-08-12

**Tracking:** `temporal_projects-7fn`

## Product decision

V1 is reference-first. It ships a normative responsibility and lifecycle
contract, executable cookbooks, and a fault-conformance profile before it
freezes a broad runtime API. Small Go and Python bindings are incubating
implementations of the contract, not a general agent framework.

The primary user is the backend or platform engineer accountable for running a
long-lived coding agent with Temporal in production. That engineer needs to know
whether recovery preserved the application invariant, not merely whether a
Workflow remained healthy. Temporal SDK and framework maintainers are secondary
consumers of the contract and conformance evidence.

The first product is an internal Temporal proposal with runnable repository
artifacts. Its prose is developer-facing, but publication, support, and external
compatibility claims require separate review and explicit approval.

## Problem

Temporal durably records and retries procedure. Coding agents also own external
state that is not contained by Event History: vendor sessions, processes,
workspaces, repositories, tool effects, provider resources, credentials,
artifacts, and consumer-visible events. A retry can therefore recover the
Temporal procedure while duplicating an effect, launching a competitor,
accepting a stale writer, leaving an orphan, or reporting cancellation before
authority is revoked.

Existing agent examples often demonstrate a successful run. V1 instead teaches
and verifies the boundary at which the Worker, agent process, destination, or
acknowledgement is lost.

## Controlling claim

> Temporal durably recovers the agent procedure. The reference patterns show,
> and the conformance profile verifies, where stable identity, application
> fencing, destination idempotency or reconciliation, explicit cancellation,
> and bounded recovery policy are additionally required for correctness under
> declared faults.

Conformance is always scoped to recorded versions, inputs, destination
semantics, and fault boundaries. V1 does not claim generic exactly-once effects,
a universally durable agent, broad vendor compatibility, cross-host recovery,
or a production failure rate. The [guarantee ledger](../guarantees.md) remains
the authoritative index of bounded repository claims.

## Success metric

The primary outcome is **first trustworthy recovery**:

> From a fresh checkout, one documented command produces an independently
> verified unsafe failure, a protected pass, preserved raw evidence, and
> successful Temporal history replay for the same declared fault.

The Python and Go exemplars must both satisfy the common contract. Every
cookbook must trace normative statements to admitted evidence. Pilot runs record
time to this outcome, but v1 sets no unevidenced time threshold.

Default product coverage excludes controlled-host timing profiles and still
requires at least 80% coverage over the topology correctness, safety, liveness,
diagnosability, replay, and fail-closed audit paths. The separate timing target
retains its registered latency oracles for future controlled compute. That
population work does not block v1 because performance comparison and production
latency estimates are explicit v1 anti-goals.

## Maturity model

### Normative

The following requirements have evidence across multiple cases, implementations,
or destination classes:

- mint stable logical session, turn, operation, and effect identities before
  ambiguous external work;
- keep delivery attempt, Worker, process, and vendor-call identity observational;
- carry logical identity unchanged through Activity retry;
- state which layer supplies every guarantee: Temporal, application,
  destination/provider, or operating system;
- require an effect protocol at the accepting destination or an explicit,
  bounded reconciliation outcome;
- separate cancellation request, authority revocation, stop delivery,
  acknowledgement, and process exit;
- place external-process lifetime outside a replaceable Worker and use exact
  start-or-attach identity at tested single-host CLI boundaries;
- commit monotonic generation/capability fencing before replacement, require
  current-owner effect/result/completion, and revoke before stop delivery;
- own one durable retry budget, admission rule, poison disposition, and progress
  policy; and
- treat stream publisher identity as delivery identity: carry a stable logical output ID,
  mark retry generations, await flush before declaring a prefix admitted, reconstruct on
  reset, and acknowledge the terminal offset before Workflow close; and
- publish large artifacts as content-addressed bytes, then an immutable logical reference,
  then a stable consumer acknowledgement; reconcile pending/unreachable records explicitly;
  and preserve only typed events and compact references in Event History. SDK External
  Storage claims are transport references, not application acknowledgements.

### Experimental

Carrying session ownership, approval state, and stream position through
Continue-As-New is also experimental. Finding 0008 records one controlled
continuation in the pinned Python exemplar; it does not meet the general
promotion threshold below.

Provider-specific CLI compatibility, session/transcript persistence,
authentication lifecycle, protocol-native APIs, and cross-host supervisor
recovery remain experimental even though the common ownership mechanism is
normative at the recorded Claude Code and Codex CLI boundaries.

### Excluded from v1

- bundled or hosted agent runtime;
- Temporal Server changes;
- public vendor compatibility promises;
- cross-host discovery, attachment, or hostile-process containment;
- generic sandbox provider;
- vendor authentication or transcript-storage management;
- performance leaderboard or production failure-rate estimate; and
- full Python/Go API-shape parity.

## Portable protocol

The normative authority is a versioned, language-neutral state machine under
`specs/coding-agent-durability/v1/`. Go and Python expose idiomatic bindings;
they need behavioral and fixture parity, not identical class hierarchies.

### Identities

| Identity | Stability | Purpose |
| --- | --- | --- |
| `session_id` | Across the logical agent session | Correlates turns, ownership, events, and artifacts |
| `turn_id` | Across every delivery of one requested turn | Prevents a vendor session from being mistaken for turn authority |
| `operation_id` | Across retries of one external operation | Correlates provider, process, and completion receipts |
| `effect_id` | Across deliveries of one destination mutation | Supports destination deduplication or reconciliation |
| `generation` | Monotonically advances on replacement | Prevents ABA owner-label reacquisition |
| `owner_capability_digest` | Changes with authority; raw capability stays outside history/evidence | Binds authoritative transitions without publishing a bearer secret |

Temporal Activity attempt, task token, Workflow Run ID, Worker ID, vendor
session/thread ID, PID/start identity, and physical destination receipt remain
recorded observations. None replaces the logical identities above.

### State scope

State is scoped to one `(session_id, turn_id)`; `operation_id` and `effect_id`
are stable children of that turn. Generation is monotonic within the turn.
Effect identity must additionally be unique in the accepting destination's
declared namespace.

Turn lifecycle and generation authority are separate axes:

- lifecycle: `claimed`, `starting`, `running`, `completing`, `succeeded`,
  `canceled`, or `unresolved`;
- authority: one generation is `current`; every superseded or terminal
  generation is `revoked`.

`succeeded`, `canceled`, and `unresolved` are terminal. `revoked` is not a
synonym for `canceled`: replacement revokes the old generation while the turn
continues, whereas cancellation revokes authority and terminates the turn. An
implementation may add substates, but it may not collapse `unresolved` into
success or silently repeat work whose external state cannot be established.

### Normative transition table

| Operation | Prior lifecycle | Authority precondition | Next lifecycle and authority | Replay rule |
| --- | --- | --- | --- | --- |
| `claim` | Absent | Authenticated application coordinator; stable IDs unused | `claimed`, generation 1 `current`; mint capability and store only its digest | Same operation ID and request hash returns the original receipt; changed content conflicts |
| `begin_start` | `claimed` | Current generation and capability | `starting`, same generation `current` | Exact replay returns the stored start receipt |
| `register` | `starting` | Current generation/capability and exact process or provider identity | `running`, same generation `current` | Same identity returns its receipt; a different identity conflicts |
| `attach` | `starting`, `running`, or `completing` | Exact current generation and discoverable executor identity | No state change | Returns the existing executor/result handle; never launches |
| `replace` | Any nonterminal lifecycle | Authenticated coordinator, expected current generation, and explicit replacement policy | Atomically revoke generation N, mint N+1, enter `starting` | Exact replay returns N+1; a later or conflicting expected generation is stale/conflict |
| `observe_progress` | `running` | Current generation/capability | No state change; append progress receipt | Exact replay returns the stored receipt |
| `publish_effect_receipt` | `running` | Current generation/capability plus declared destination capability | No state change; append immutable effect receipt | Same effect ID and request hash returns its receipt; changed content conflicts |
| `publish_result` | `running` | Current generation/capability and result agrees with declared system-of-record checks | `completing`, same generation `current` | Exact replay returns the candidate-result receipt |
| `complete` | `completing` | Current generation/capability and matching candidate-result receipt | `succeeded`; revoke current generation | Exact replay returns the terminal receipt; stale generation is rejected |
| `cancel` | Any nonterminal lifecycle | Authenticated coordinator and expected current generation | Atomically `canceled`; revoke current generation before stop delivery | Exact replay returns the terminal cancellation receipt |
| `mark_unresolved` | Any nonterminal lifecycle | Authenticated coordinator, expected current generation, and recorded ambiguity reason | Atomically `unresolved`; revoke current generation | Exact replay returns the unresolved receipt; no automatic repeat follows |
| `record_stop_delivery` / `acknowledge_stop` | Revoked generation or canceled turn | Exact target session, turn, generation, and process/provider identity | No lifecycle change; append separate delivery/ack receipts | Exact replay returns the stored receipt; a different target conflicts |

`claim`, `replace`, `cancel`, and `mark_unresolved` are coordinator operations,
not bearer-capability operations. `claim` bootstraps the first capability;
`replace` atomically mints its successor. The application must authenticate the
coordinator independently of agent generations. Every executor-originated
authoritative transition requires the raw current capability at the enforcing
boundary; only its digest may appear in Workflow history or portable evidence.

### Guarded transitions

The contract defines `claim`, `begin_start`, `register`, `attach`, `replace`,
`observe_progress`, `publish_effect_receipt`, `publish_result`, `complete`,
`cancel`, `mark_unresolved`, `record_stop_delivery`, and `acknowledge_stop`.
Each authoritative transition must:

1. name the complete logical identity and current generation;
2. satisfy the coordinator or executor authorization rule in the transition
   table;
3. validate the expected prior state atomically;
4. return an immutable receipt or a typed stale/canceled/conflict result; and
5. preserve enough evidence to reconstruct the decision independently.

A replacement commits generation `N+1` before it starts a new executor. A
cancellation commits terminal revocation before best-effort process stopping.
Completion is conditional on current authority and cannot rely on logical
Activity ID alone.

### Effect capabilities

The protocol declares, rather than invents, the destination capability used by
an operation:

- atomic idempotency key plus stored receipt;
- transactional unique effect identity;
- stable message identity with declared retention;
- serialized correlation lookup before a non-idempotent call;
- conditional/versioned Git mutation plus receipt reconciliation;
- content-addressed blob plus atomically published stable reference; or
- explicit unresolved/manual-reconciliation disposition.

An application pre-check is not an atomic destination guarantee when same-ID
callers can race.

## Incubating SDK/contrib surface

The portable schemas define truth. `contrib/codingagent/` supplies small,
incubating Go and Python surfaces for:

- immutable identity and authority values;
- validated state transitions and typed receipts;
- destination capability declarations;
- effect execution or reconciliation results;
- structured event envelopes and artifact references; and
- adapters into the shared conformance fixtures.

Provider-specific model loops, CLIs, sandboxes, authentication, and transcript
stores remain outside the kernel. Semantic choices such as whether a process is
healthy, whether a wait is legitimate, or whether an ambiguous effect is safe to
repeat are supplied explicitly by a model, operator policy, or destination
contract; the orchestration library does not hide them in keyword rules or
heuristic scores.

## Cookbook series

Every cookbook contains an unfaulted path, a distinguishing unsafe control, a
protected path, invalid-evidence fixtures, exact named fault barriers, an
independent destination/workspace oracle, retained evidence, replay where
applicable, and a responsibility split.

1. **Temporal-native agent loop and replay** — model/tool Activities, approval,
   typed output, stream state, Continue-As-New, replay, and ambiguous tool
   effects.
2. **Effect-safe tools** — idempotent API, non-idempotent API reconciliation,
   transactional database, Git, message publication, and the observed large-artifact
   blob/reference/acknowledgement protocol.
3. **External CLI ownership** — direct relaunch and resume-only controls
   followed by start-or-attach and fenced completion; normative contract at the
   tested single-host boundaries, with provider compatibility bounded.
4. **Cancellation and cleanup** — durable revocation, exact process targeting,
   stop delivery, acknowledgement, descendants, and terminal races.
5. **Sandbox lifecycle** — provider operation receipts, attached-writer fencing,
   snapshot lineage, and orphan reconciliation.
6. **Bounded recovery policy** — retry ownership, outage catch-up, admission,
   backpressure, poison quarantine, and progress deadlines.

## Conformance profile

Conformance is binary and evidence-bearing; it is not a score or leaderboard.
For each applicable case an integration must provide:

1. an unfaulted `valid-pass`;
2. an unsafe `valid-fail` that exposes the named invariant violation;
3. a protected `valid-pass`;
4. malformed, missed-boundary, wrong-identity, and contradictory evidence that
   the oracle rejects as invalid;
5. exact named barriers rather than sleeps that choose the outcome;
6. stable logical identities, source/configuration hashes, process identity,
   UTC event sequence, and native history;
7. independent destination or workspace observations; and
8. replay of every captured Temporal history.

Concurrency-sensitive development boundaries repeat at least three times. A
public performance or rate claim requires a separate preregistered population,
matched execution order, retained invalid/failing trials, and uncertainty
analysis.

The implementation extends
`benchmarks/agent-durability/conformance/` and reuses the existing append-only
evidence writer and independent oracle. Adapter logs may explain a verdict but
may not choose it.

## Evidence and claim matrix

| Product requirement | Admitted observation | Product use | Current maturity |
| --- | --- | --- | --- |
| Separate Temporal completion from external-effect cardinality | [Finding 0004](../findings/0004-one-temporal-completion-can-hide-two-effects.md) recorded two physical effects in all 18 unsafe trials across six destination classes and one in all protected trials through destination-specific mechanisms | Stable `effect_id`, capability declaration, reconciliation receipt, effect-safe tools cookbook | Normative |
| Preserve native agent procedure without overstating tool safety | [Finding 0008](../findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md) restored completed model/tool results while unsafe tool effects duplicated; one controlled Continue-As-New carried approval/stream state | Python native-loop exemplar and replay requirement; Continue-As-New state transfer remains experimental | Normative for completed-step replay at the pinned exemplar; continuation experimental |
| Revoke authority separately from cancellation procedure | [Finding 0006](../findings/0006-cancellation-requires-application-revocation.md) observed all Temporal-only controls mutate after cancellation and no protected post-revocation mutation | Cancellation state machine, exact cleanup recipe | Normative |
| Keep sandbox operation identity and ownership separate | [Finding 0009](../findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md) distinguished outer Update identity, inner provider receipts, attached-writer fences, and orphan reconciliation | Sandbox cookbook and separate resource/session contracts | Normative for tested boundaries |
| Keep large artifact bytes, references, and consumption acknowledgements separate | [Finding 0024](../findings/0024-large-artifacts-need-reference-and-acknowledgement-protocols.md) admitted 36 blob/reference/Activity/ack/External Storage runs: all 18 protected arms converged and all nine distinguishing unsafe reference/ack/offload controls duplicated | Typed artifact reference and acknowledgement pattern; effect-safe-tools cookbook; External Storage remains transport-only | Normative for the tested application protocol; remote stores and SDK offload are bounded/experimental |
| Reject direct CLI relaunch as a durability integration | [Finding 0010](../findings/0010-direct-claude-activity-retry-duplicates-turns-and-effects.md) observed two sessions and effects in every faulted direct-Claude trial | External CLI unsafe control | Normative negative guidance at pinned version |
| Reject transcript identity as turn authority | [Finding 0019](../findings/0019-claude-resume-preserves-session-identity-not-effect-safety.md) observed one resumed session UUID but two effects in every faulted trial | Resume-only unsafe control and distinct `turn_id`/generation | Normative negative guidance at pinned version |
| Bound retry, admission, poison, and progress policy in application code | [Finding 0013](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md) admitted 540 matched pairs (1,080 executions): for each system, all 360 unfaulted/protected executions passed and all 180 unsafe executions distinguished, while attributing the policies to the application | Portable recovery-policy contract and cookbook | Normative; performance remains single-host and separate |
| Compose bounded recovery with both Temporal topology shapes | [Finding 0016](../findings/0016-recovery-dynamics-controls-distinguish-with-bounded-catchup.md) admitted 52 canonical development runs with distinguishing controls and replay | Go recovery exemplar; do not prescribe topology on performance grounds | Normative mechanism, not topology ranking |
| Start-or-attach and fence an external vendor process | [Findings 0020](../findings/0020-application-fenced-claude-supervisor-survives-worker-loss.md) and [0021](../findings/0021-codex-thread-resume-is-not-turn-authority.md) admit matched hermetic and authenticated Claude/Codex controls and protected arms | External CLI ownership contract and cookbook | Normative for tested single-host boundaries; supervisor/host loss and cross-host routing remain unproven |
| Pinned Codex CLI conformance | [Finding 0021](../findings/0021-codex-thread-resume-is-not-turn-authority.md) records matched Codex CLI `0.147.0` unsafe, resume-only, and fenced populations with replay | CLI conformance target; App Server excluded | Observed mechanism at the pinned version, not a general support claim |

## Promotion policy

A pattern becomes normative only when admitted live evidence contains a
distinguishing unsafe control, protected arm, exact boundary, independent oracle,
source/version pins, replay where applicable, and at least three repetitions for
concurrency-sensitive behavior. A vendor-neutral abstraction additionally needs
the same boundary and contract in at least two independent implementations,
vendors, or destination classes.

The common external-CLI ownership mechanism meets that promotion rule through
the authenticated Claude Code and Codex CLI populations in Findings 0020 and
0021. This does not promote generic vendor compatibility or product support.

Structurally tested or single-implementation behavior remains experimental.
Public performance and failure-rate claims require a separate preregistered
population and uncertainty analysis. Documentation or a passing demo cannot
promote a guarantee.

## Package layout

```text
docs/product/coding-agent-durability-v1.md
specs/coding-agent-durability/v1/README.md
specs/coding-agent-durability/v1/schema/{identity,transition,event,evidence}.schema.json
specs/coding-agent-durability/v1/fixtures/{valid,invalid}/
contrib/codingagent/go/
contrib/codingagent/python/pyproject.toml
contrib/codingagent/python/src/temporal_coding_agent/
docs/product/fault-tested-coding-agent-cookbooks.md
cookbooks/coding-agents/presentation/
cookbooks/coding-agents/01-native-agent-loop/
cookbooks/coding-agents/02-effect-safe-tools/
cookbooks/coding-agents/03-external-cli-ownership/
cookbooks/coding-agents/04-cancellation-and-cleanup/
cookbooks/coding-agents/05-sandbox-lifecycle/
cookbooks/coding-agents/06-bounded-recovery/
benchmarks/agent-durability/conformance/cmd/conformance/
benchmarks/agent-durability/conformance/{profile,oracle,evidence}/
Makefile target: coding-agent-conformance
```

Existing experiments and raw evidence stay append-only in place. Product
artifacts reference them; they do not bulk-move, rewrite, or hide correction
lineage.

## Anti-goals

V1 does not build a general agent or model framework, hosted runtime, mutable
agent control UI, deployment platform, generic sandbox provider, vendor
authentication system, or transcript store. A read-only evidence explorer may
render independently verified outcomes, but it cannot become an oracle or
promote a claim. V1 does not hide semantic policy in deterministic heuristics,
promise full Python/Go parity, rank durability systems or Temporal topology, or
extract a one-off experiment mechanism before a second real use justifies the
shared boundary.

## Roadmap and gates

The dependency-ordered implementation is tracked by children of
`temporal_projects-7fn`:

1. freeze this specification;
2. define the versioned protocol and fixtures;
3. build the shared conformance profile;
4. implement the Go and Python bindings against it;
5. implement all six cookbooks; and
6. run the fresh-checkout integration, independent review, race, coverage,
   evidence, replay, and claim audit.

Step 6 uses the ordinary-CI correctness coverage gate. Controlled-host timing
profiles remain a separate, opt-in research gate and cannot promote or renew a
timing claim without fresh append-only evidence, replay, audit, and the
preregistered host envelope.

No implementation result changes the guarantee ledger until admitted evidence
supports the bounded claim. No commit, push, publication, or external artifact
is authorized by this specification.
