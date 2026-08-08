# Temporal-native agent-loop baseline

## Question

What does Temporal recover when the OpenAI Agents SDK loop is inside a
Workflow and each model or tool call is a Temporal Activity, and which safety
properties still require application or destination mechanisms?

## Invariant

At most one authorized logical operation may be accepted for a turn, and its
structured result must agree with independently observed workspace and
destination state.

The logical session, turn, and effect identities are minted by the Workflow and
remain stable across Activity attempts. Temporal attempt numbers, Worker
processes, model call IDs, and physical destination attempt IDs are observations,
not logical identity.

## Failure boundaries

The harness uses named barriers and durable Workflow states rather than delays:

- `client-start-acknowledged`: a one-use client interceptor has received a
  successful server response but withholds it from the caller;
- `model-response-built`: a model Activity has constructed its response but has
  not completed to Temporal;
- `tool-effect-committed`: the controlled destination has committed the physical
  effect but the tool Activity has not completed to Temporal;
- `workflow-result-built`: the Workflow publishes the correlated result event
  and waits on a release Signal before it may return;
- `awaiting-approval`: cancellation or termination is delivered while no model
  or tool Activity has started.

Activity barrier arrivals identify the logical operation, Activity attempt,
Worker process, and a one-use arrival token. Workflow-state boundaries are
observed through Query or the Workflow event stream and released by Signal.

## Oracle

The common append-only evidence writer records raw events, authority state,
destination attempts, the exact fault boundary, process observations, Temporal
Event History, effective input, and pinned versions. The independent common
oracle evaluates the resulting bundle after the adapter exits.

The experiment also verifies the accepted result's artifact hash and destination
receipt directly against the fixture repository and SQLite destination. The
unsafe destination arm must expose more than one applied physical attempt after
completion loss; the protected arm must retain one applied logical effect while
recording every delivery attempt.

## Responsibility split

- Temporal records Workflow decisions, schedules and retries model/tool
  Activities, restores completed Activity results during replay, and exposes
  Event History.
- Application code mints stable logical identities, correlates model/tool/result
  records, chooses cancellation policy, and carries state across
  Continue-As-New.
- The controlled destination enforces effect idempotency in the protected arm.
  Activity retries alone do not provide exactly-once effects.
- The OpenAI Agents SDK validates the typed model result and drives the tool
  loop. The hermetic fixture model removes model judgment from the oracle.

## Falsifier

The conclusion is false or narrower if an accepted run contains duplicate or
stale-authority effects, its result disagrees with workspace/destination state,
fault placement depends on time, evidence lacks an identity or source version,
the independent oracle accepts the unsafe duplicate-effect control, replay
reissues a completed model/tool turn, or Continue-As-New loses session, approval,
stream, or ownership state.

## Pinned inputs

- Temporal Server `1.31.2` / local test-server CLI `1.8.2`
- Temporal Python SDK `1.31.0`
- OpenAI Agents SDK `0.19.4`
- reviewed `temporal-sa/durable-agentic-harness` commit
  `4afef65defcd8e70d6e794936320e4d7513fd365`
- reviewed Temporal Python SDK commit
  `84b519e0ff407b049da88ac7d1711f110494ff4d` (tag `1.31.0`)
- reviewed Temporal Python samples commit
  `cae48d291ac28f92e81591f1aa0c2b5d956b7bca`

## Run commands

Unit and integration tests:

```bash
uv run pytest --cov=temporal_native --cov-branch
```

Append-only live evidence (the target directory must not exist):

```bash
make evidence-temporal-native \
  EVIDENCE_ROOT=experiments/durable-vendor-sessions/temporal-native/evidence/<suite>
```

## Observed result

The admitted suite is
[`temporal-native-20260807-v3`](evidence/temporal-native-20260807-v3). Its three
unsafe trials each recorded two applied physical effects and received
`valid-fail` with `duplicate_physical_effect`. Its three destination-idempotent
trials each recorded two deliveries, one applied effect, and `valid-pass`. No
bundle was invalid.

The integration suite also exercises start-response loss, model Activity
completion loss, Worker death after result construction, cancellation versus
termination, event delivery across Continue-As-New, and captured-history replay.
These tests use exact barriers or durable states and negative controls; only the
ambiguous-effect matrix is currently emitted as append-only common-oracle
evidence.

V1 and v2 are preserved but superseded. Review found that v1 hashed only
`worker.py` and used a semantic adapter version. V2 corrected provenance, then
the Python sources were refactored during final review. V3 hashes the handed-off
Python package and both Go adapter sources without changing the trial outcomes.
