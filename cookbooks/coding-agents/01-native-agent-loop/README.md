# Temporal-native agent loop

This cookbook is a thin executable guide to the admitted
[Finding 0008](../../../docs/findings/0008-temporal-native-agent-loop-recovers-structure-not-effects.md)
and its
[Temporal-native experiment](../../../experiments/durable-vendor-sessions/temporal-native/README.md).
It points at the experiment instead of copying its implementation or rewriting its raw
evidence.

## Question

What does Temporal recover when an OpenAI Agents SDK loop runs inside a Workflow with
model Activities and tool Activities, and what safety must the application or effect
destination still provide?

## Invariant

At most one authorized logical operation may be accepted for a turn, and the typed result
must agree with independently observed workspace and destination state. The Workflow owns
stable session, turn, effect, generation, and owner identities; Activity attempts, model
call IDs, Worker processes, and physical destination attempts are observations rather than
logical identity.

## Architecture slice

The Workflow is deterministic orchestration. The OpenAI Agents plugin schedules each model
call as an Activity, adapts `apply_fixture_change` into an agent tool Activity, and returns a
Pydantic `TurnResult`. Before any model or tool call, an optional approval gate waits for a
Workflow Signal. Lifecycle items are published through `WorkflowStream`; its serializable
stream state, the still-closed approval state, and the unchanged owner capability cross a
forced Continue-As-New transition.

The controlled fixture model is hermetic. This proves orchestration and recovery behavior,
not the durability or repeatability of a live model provider's token stream.

## Failure boundary

The central ambiguous-effect fault is exact: the first Worker reaches
`tool-effect-committed` after SQLite has committed the effect but before the tool Activity
has completed to Temporal. The harness observes the barrier, confirms the first destination
attempt, kills that Worker, starts a replacement, and allows Temporal to retry delivery.
There is no sleep-based failure window.

The experiment also places exact or durable boundaries after a StartWorkflow response, a
constructed model response, and the Workflow's `result_built` event, plus an
`awaiting_approval` cancellation/termination boundary. Those cases are integration-test
observations; only the ambiguous-effect matrix is admitted to the common oracle.

## Oracle

The critical runnable path checks that one complete typed agent turn correlates the
Workflow result with the destination receipt and artifact hash, then performs history replay
with the same streaming plugin parameters. The admitted common-oracle bundles additionally
compare two physical deliveries at the exact effect boundary:

- The unsafe control must apply both physical attempts and receive
  `valid-fail / duplicate_physical_effect`.
- The destination-idempotent arm must record both deliveries, apply the stable logical
  effect once, and receive `valid-pass`.

`run.sh check` reads the admitted bundle without modifying it. It verifies the exact sealed
tree and every manifest hash, reconstructs the unsafe/protected outcome from the raw barrier,
event, authority, destination, and Temporal-history exports, resolves the cited source, and
checks that the critical runner targets the typed-result/history-replay integration test.
The six legacy history exports are inspected but not individually replayed by this cookbook;
the critical path captures and replays a current history with the registered Workflow.

## Fresh-checkout run

Prerequisites are Python 3.12 and [`uv`](https://docs.astral.sh/uv/). From the repository
root, run exactly:

```bash
./cookbooks/coding-agents/01-native-agent-loop/run.sh all
```

`uv run --locked` creates the pinned experiment environment when needed. The critical path
starts Temporal's local time-skipping test environment; it does not require an OpenAI key or
an already-running Temporal service. The modes can also be run separately:

```bash
./cookbooks/coding-agents/01-native-agent-loop/run.sh check
./cookbooks/coding-agents/01-native-agent-loop/run.sh critical
```

For the complete experiment suite with its 80% coverage gate, run:

```bash
cd experiments/durable-vendor-sessions/temporal-native
uv run --locked pytest --cov=temporal_native --cov-branch
```

Do not point the live evidence command at an existing directory: evidence capture is
append-only and deliberately refuses to overwrite a suite.

## Evidence

The admitted raw suite is
[`temporal-native-20260807-v3`](../../../experiments/durable-vendor-sessions/temporal-native/evidence/temporal-native-20260807-v3).
Each of its six trial directories retains the Temporal history, exact barrier record,
process controls, destination snapshot, effective input, source hashes, common events, and
independent verdict. The
[Workflow source](../../../experiments/durable-vendor-sessions/temporal-native/temporal_native/workflow.py)
shows approval, typed result, stream state, tool adaptation, and Continue-As-New. The
[process-recovery tests](../../../experiments/durable-vendor-sessions/temporal-native/tests/test_process_recovery.py)
and
[stream-continuation test](../../../experiments/durable-vendor-sessions/temporal-native/tests/test_stream_continuation.py)
cover the other recovery boundaries.

## Observed result

All three unsafe trials recorded two deliveries and two applied physical effects; each was
scored `valid-fail / duplicate_physical_effect`. All three destination-idempotent trials
recorded two deliveries but one applied logical effect; each was scored `valid-pass`. No
bundle was invalid.

After Worker death, Temporal replay restored completed model and tool Activity results
without reissuing them. An incomplete streaming model Activity was retried. A subscriber
followed Continue-As-New and observed the full pre/post-transition event sequence with one
owner capability while approval remained closed. Captured history replay passed with the
original streaming parameters and rejected missing parameters as nondeterministic during
development.

Continue-As-New is an experimental boundary here: it was forced once before the agent ran.
It demonstrates state handoff at that controlled transition, not production-sized history
management, consumer acknowledgement, post-close stream retention, or exactly-once UI
delivery.

The separate [Workflow Stream retry experiment](../../../experiments/workflow-stream-retry/README.md)
now supplies the missing retry/acknowledgement pattern for one open run: keep a stable
logical output ID, expose Activity-attempt/publisher identity, emit a retry/reset marker,
await `flush()` before calling a prefix admitted, reconstruct after reset, and keep the
Workflow open through acknowledgement of the terminal offset. That result does not yet
compose retry with this cookbook's Continue-As-New boundary or establish post-close
retention.

## Responsibility split

- Temporal records Workflow decisions, Signals, Activity results and retries, and run
  transitions; replay reconstructs that recorded orchestration.
- The OpenAI Agents plugin maps the model/tool loop onto Temporal primitives and validates
  the typed result shape.
- Application code mints stable logical identities, correlates results, carries approval and
  stream state across Continue-As-New, selects start/cancellation policy, and performs
  retry-safe cleanup.
- The destination transaction deduplicates the stable logical effect in the protected arm.
  Temporal Activity retries recover delivery; they do not make ambiguous external effects
  exactly once.

## Falsifier

The conclusion is false or narrower if replay reissues a completed model or tool Activity;
a continued run changes ownership or loses approval or stream state; the typed result no
longer matches the destination; a protected trial applies the logical effect twice; the
unsafe negative control stops exposing the duplicate; or a fresh, source-pinned three-trial
suite changes these verdicts. It is also invalid if the fault is not bracketed by the first
and second destination observations or if cancellation cleanup is attributed to forced
termination.
