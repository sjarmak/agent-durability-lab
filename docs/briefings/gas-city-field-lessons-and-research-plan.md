# Reliable agent execution on Temporal

**Gas City field lessons, Agent Durability Lab findings, benchmark methods, and remaining research**
**Status:** Shareable working brief, 2026-08-07

## The short version

Gas City supplied field evidence that the hard part of durable agent execution
is not restarting a procedure. It is preserving the identity and authority of
work whose effects continue outside the durability system.

The strongest lessons are:

1. One logical task can fragment into several live writers unless claims and
   completions are fenced by generation.
2. A durable retry can make the application less correct when the original
   child process or external effect survived the failed Worker.
3. Stable identity must exist before the first heartbeat or progress update.
4. Acknowledgement and cancellation are authority transitions, not message
   delivery events.
5. Durable execution preserves bad instructions as faithfully as good ones.
   Promotion gates, evidence, and repair paths are part of correctness.
6. Putting Temporal around an operation is useful only when Temporal owns a
   meaningful procedure. Wrapping an opaque scheduler or long-lived CLI creates
   two lifecycle owners and poor evidence.

The Agent Durability Lab has reproduced the identity, effect, stale-authority,
and cancellation boundaries under controlled failure. The next major step is a
mechanism-neutral comparison of Temporal, Restate, DBOS Go, and a PostgreSQL
queue/lease/outbox. The comparison will hold the agent, destination, fault
schedule, evidence, and oracle fixed.

## How to read the evidence

This brief uses three evidence labels:

| Label | Meaning |
| --- | --- |
| **Field observation** | An incident, failed promotion, or bounded canary recorded by Gas City. It may include real operational complexity that was not isolated experimentally. |
| **Controlled reproduction** | A named fault was injected at a barrier, repeated, and checked by a machine-readable oracle in the Agent Durability Lab. |
| **Design conclusion** | The smallest mechanism consistent with the observations. It remains falsifiable and may change when a stronger experiment disagrees. |

Temporal documentation and source are inputs to experiment design. They are not
treated as proof of end-to-end application behavior.

## What came directly from Gas City

### 1. A single work item fragmented into competing writers

**Field observation.** On 2026-06-18, one Gas City work item was concurrently
claimed by four workers. Three edited the same branch and left divergent
uncommitted work. The work item ID remained singular while execution identity
fragmented underneath it.

This incident motivated an enforced claim lock, generation/fence identity,
artifact binding, and a typed outcome. A free-text completion message was not
enough to prove which executor produced which artifact under which authority.

**Design conclusion.** A logical work ID is not an execution capability. Every
authoritative mutation and completion needs the current generation or an
equivalent opaque fence, checked at the destination that accepts the mutation.

### 2. The first Temporal pilot stopped at the wrong ownership boundary

**Field observation.** The initial maintenance pilot ran an Activity that
created a work item, invoked Gas City's dispatch command, and returned. A
separate scheduler then owned the actual agent lifecycle. Temporal durably
recorded coordination around work whose start, progress, retry, and completion
were controlled elsewhere.

This produced two lifecycle owners and no reliable answer to whether an Activity
retry should start, attach, or reconcile the external execution.

**Design conclusion.** An Activity must own a real execution episode or call a
service with an explicit start-or-attach protocol. Merely submitting work to
another scheduler does not transfer enough identity or state for safe recovery.

### 3. Stable identity had to precede heartbeat state

**Field observation and bounded failure test.** Gas City's worker-kill test sent
`SIGKILL` to a Worker while its independently running agent kept writing. The
retry resolver ran again, but only one external session was created when both
attempts used the same logical session identity. A delayed obsolete claim token
was rejected. This test used a fixture agent and file store on one host.

Heartbeat details helped the retry resume progress, but heartbeat state was not
the source of duplicate prevention. The session identity had to be derivable
before the first heartbeat, including the window before child registration.

**Design conclusion.** Stable identity and authority belong in application
state. Heartbeats are useful progress and liveness observations, not a complete
external-process identity protocol.

### 4. The first promotion candidate exposed protocol defects while Temporal stayed healthy

**Field observation.** A bounded OutcomeReady promotion candidate exposed four
defects:

- shared mail-store contention;
- correlation metadata naming a Workflow execution that did not exist;
- an observer confusing a formula step with its source work item; and
- legitimate terminal paths that emitted no OutcomeReady envelope.

The failed gate was retained and analyzed. A later repair run delivered the
stranded outcomes and corrected their correlation, while the source root
remained blocked for the right domain reason.

**Design conclusion.** Healthy Temporal infrastructure does not imply a healthy
application protocol. Promotion gates need to check work-store state, external
artifacts, correlation, and acknowledgement in addition to Event History.

### 5. Durable execution made wrong configuration persistent and inspectable

**Field observation.** A later disposition review found that an OutcomeReady
canary configuration had remained present after its intended test window. It
continuously failed against the wrong store reference and accumulated
unacknowledged conflicts. The correct ruling was to disarm it and preserve the
invalid records, not drain or reinterpret them.

This record also corrected older status language that described continuous
production delivery. The reviewed current state was shadow/shadow with no
outcome queue poller and no authorized production mutations.

**Design conclusion.** Durable retry makes a bad instruction persistent and
observable; it does not make the instruction correct. Configuration identity,
promotion state, and an operator-visible quarantine or repair path are part of
the application contract.

### 6. Acknowledgement was designed as a fenced state transition

**Field observation and tested design.** Gas City tied outcome acknowledgement
to the exact coordinator incarnation and current fence. Delayed acknowledgement
from a prior coordinator was rejected instead of being accepted because it
referred to the same logical work item.

**Design conclusion.** Delivery answers whether a message was sent.
Acknowledgement answers whether the current authority durably incorporated its
meaning. The latter must be conditional on identity and state.

### 7. Some workloads were deliberately left outside Temporal

**Field observation.** A short maintenance job remained cron plus a lockfile. It
needed to run during a Temporal outage and did not have a useful replayed state
machine. Gas City also rejected wrapping its always-on interactive coordinator
as one opaque Workflow or Activity. That would durably record that a process
started, heartbeated, and exited while hiding the decisions and effects that
matter.

**Design conclusion.** Durable execution is not automatically durable
orchestration. A Workflow earns its cost when it owns meaningful ordering,
waiting, compensation, or human gates. Some independent maintenance and
availability functions should retain a recovery path outside Temporal.

### The boundary Gas City arrived at

The field work produced a three-part ownership model. The lab treats it as a
tested hypothesis rather than a universal architecture:

| Owner | State or behavior |
| --- | --- |
| Temporal | Procedure history, ordering, retries, durable waits, cancellation procedure, fan-out, and human gates |
| Application work store | Work identity, readiness, dependencies, claims, generations, fences, artifacts, outcomes, and acknowledgements |
| Activity and executor code | Agent processes, files, Git, tests, API calls, messages, and other external effects |

The critical boundary is between the second and third rows. The work store can
revoke an executor, but the destination must check that revocation before it
accepts a mutation. Temporal can retry an Activity, but it cannot infer whether
the prior process or effect survived.

## How the lab sharpened those lessons

Gas City supplied incidents and architecture hypotheses. The lab then isolated
the dangerous boundaries and added unsafe controls.

| Gas City antecedent | Controlled lab result | Responsibility exposed |
| --- | --- | --- |
| Competing claims and divergent Git writers | [Finding 0001](../findings/0001-worker-death-surviving-agent.md): unsafe retry created two executors and two accepted effects; reattachment and fenced replacement did not | Temporal redelivered; the application supplied stable identity and fencing |
| Start-or-attach needed after Worker death | [Finding 0002](../findings/0002-launch-decision-is-not-process-liveness.md) and [Finding 0005](../findings/0005-launch-pending-does-not-identify-process-reality.md): the same `launch_pending` record represented no child or a live unregistered child | The application needed process discovery, conditional replacement, and stale registration rejection |
| Stale coordinator or executor completion must fail closed | [Finding 0003](../findings/0003-activity-id-completion-is-not-attempt-scoped.md): task-token completion rejected a stale attempt, while completion by logical Activity ID accepted it | Attempt identity came from the task token; by-ID callers required an application capability fence |
| External effects outlive a lost Activity completion | [Finding 0004](../findings/0004-one-temporal-completion-can-hide-two-effects.md): 18 unsafe trials recorded one Temporal completion but two physical effects | Deduplication, idempotency, reconciliation, or transactions came from the application and destination |
| Cancellation must revoke the exact owner | [Finding 0006](../findings/0006-cancellation-requires-application-revocation.md): six Temporal-only controls closed as canceled while the detached agent later mutated state | Temporal recorded cancellation procedure; application revocation and exact process control removed authority |

These results were observed with Go 1.25.12, Temporal Go SDK 1.47.0,
Temporal CLI 1.8.0, Temporal Server 1.31.2, and Linux amd64. Version-specific
claims should be retested when those pins change.

## The responsibility boundary supported so far

| Concern | Temporal contribution demonstrated here | Application or external contribution still required |
| --- | --- | --- |
| Procedure recovery | Durable Workflow history, Activity timeout, retry, wait, and cancellation procedure | Decide whether retry attaches, replaces, reconciles, or stops |
| Logical agent identity | Can carry a stable reference through procedure | Create and persist the session identity before ambiguous work starts |
| External writer authority | No general destination fence | Generation, capability token, lease/fence checks, or destination-native conditional write |
| External exactly-once effect | Not established | Destination idempotency, uniqueness, transaction, reconciliation, or acceptance of at-least-once effects |
| Detached process liveness | Activity heartbeat reports what the Activity can observe | Process registry/discovery, PID start identity, supervisor, or remote session service |
| Cancellation | Records and delivers Temporal cancellation according to configured semantics | Durably revoke authority, target the exact session, stop descendants, and record delivery separately from acknowledgement |
| Outcome acknowledgement | Preserves orchestration state around an acknowledgement step | Conditional state transition by the current coordinator generation |
| Repair during control-plane failure | Event History remains the procedure record when Temporal is available | An independently inspectable work store and reconciliation path |

The [guarantee ledger](../guarantees.md) contains the narrower claim and evidence
for each property.

## What the book draft adds

*Engineering Reliable Coding Agents* supplies a broader engineering framework
for these results. Its source review distinguishes controlled evidence,
directional evidence, corroborating cases, and null or conflicting results. It
also treats cases from author-operated systems as local illustrations, not
independent support. This brief follows the same rule: a book principle becomes
a lab finding only after a distinguishing experiment reproduces it here.

| Portable book claim | Consequence for this lab | Current status |
| --- | --- | --- |
| **Authority, not instruction, defines blast radius.** | A work-store fence does not protect a destination that still accepts the stale process's credential. Test the full graph of files, network routes, delegated services, policy-changing endpoints, and recovery resources reachable by each identity. | Cancellation and store fencing are reproduced. Destination-enforced credential or capability revocation remains open. |
| **Durable intent survives the worker; external effects require their own contract.** | Stable invocation identity must cross the external boundary. The destination must deduplicate, transact, converge, or force explicit reconciliation. | Reproduced across six effect classes in Finding 0004. |
| **Unknown state is a valid recovery outcome.** | When an effect may have occurred and cannot be reconciled safely, failing with an explicit unresolved claim can be more correct than reporting success or blindly retrying. | The lab records ambiguity, but a human-reconciliation terminal arm is not yet a full experiment. |
| **Recovery is a measured property.** | Every claim must name the workload, fault, protected state, expected continuation, event anchors, configuration, and omitted faults. Process restart time is not application recovery time. | Embedded in current methodology; recurring faults within one continuing run remain open. |
| **Verify the workspace or system of record, not the agent's confidence.** | Outcomes come from the destination, repository, artifact digest, process identity, and independent oracle. Agent progress and a healthy Workflow are observations, not acceptance evidence. | Enforced by current experiment oracles. |
| **Preserve typed raw events; rebuild derived views.** | Model output, tool request, external commitment, state transition, observation, and interpretation need different event types and causal identities. Large payloads may live behind content-addressed references. Missing events remain visible as trace failures. | Current evidence is structured but case-specific. A common versioned event model belongs in the observability work. |
| **A gate must have causal power.** | Test rejection, timeout, cancellation, and dependency failure. A gate earns credit only if its decision changes the next reachable application state and binds to the exact artifact or operation reviewed. | Cancellation revocation is reproduced. Promotion and future human gates need the same negative-path test. |
| **Attribute the first upstream failure the trace can support.** | A broken fixture, verifier, barrier, or environment must be ruled out before blaming Temporal, an adapter, or the agent. If evidence cannot distinguish causes, record `unresolved` and the missing observation. | Reflected in the invalid-trial policy; the failure taxonomy will evolve from retained runs. |

The book also distinguishes a retry from correction. Re-running with the same
inputs may produce a different sample, but it has learned nothing about the
failure. A replacement attempt should record which external observation changed
its admissible next action. This matters when evaluating whether an Activity
retry resumed a procedure, reconciled new evidence, or merely repeated the same
uncertain effect.

For long-running sessions, the book separates the raw event record from compacted
memory. A summary, snapshot, or progress note is a derived artifact with a schema
and policy version. It must identify the source events it incorporates and remain
rebuildable. This is directly relevant to Workflow history limits, streamed
prefixes, external artifact references, and session compatibility across Worker
deployments.

## What Temporal teams can use now

### Engineering

- Agent integrations need an explicit external-session protocol: stable logical
  ID, current generation or capability, attach/discover, replace, cancel,
  completion, and reconciliation.
- Completion by logical Activity ID and completion by task token have different
  identity strength on the tested versions. The distinction deserves prominent
  API guidance for asynchronous agent completion.
- Activity cancellation acknowledgement, application authority revocation, stop
  delivery, agent acknowledgement, and process exit are separate states. A
  single "canceled" status cannot diagnose all of them.
- Native history should correlate with application session, generation,
  destination operation, artifact, and process identity without placing large
  transcripts or artifacts in Event History.

### Developer advocacy

- A useful demonstration is not "an agent runs in a Workflow." It is "the
  Worker died, Temporal recovered, and the application either did or did not
  remain correct."
- Show the unsafe arm first. The contrast makes clear which guarantee Temporal
  supplied and which mechanism the application added.
- Avoid "exactly once" for external effects unless the named destination and
  mechanism justify it. One recorded Activity completion can coexist with two
  physical effects.
- Treat failed promotion gates as useful evidence. The first Gas City candidate
  taught more about protocol completeness than a passing happy-path demo would
  have.

### Product

- External agent sessions should be modeled as durable domain objects. Treating
  them only as long Activities hides session correlation, attempt versus logical
  identity, reattachment, and fenced completion.
- Operators need to distinguish a healthy Workflow from a healthy application:
  retrying forever, live but wedged agent, orphan process, stale writer,
  ambiguous effect, legitimate wait, and unacknowledged outcome.
- New primitives such as Streams, External Storage, Standalone Activities,
  Nexus, Serverless Workers, and Worker Versioning should be evaluated at these
  application boundaries rather than only for durable delivery.

## Benchmark methodology carried forward

CodeScaleBench, EnterpriseBench, and codeprobe exposed ways an evaluation can
produce a clean number and a false conclusion. Those lessons shape the
durability comparison as much as the Gas City failures do.

### CodeScaleBench: freeze the claim, test the verifier, and inspect every arm input

CodeScaleBench separated its frozen publication suite from its later maintained
corpus. Results name the exact suite, repository revisions, container digests,
and valid paired population. A current checkout is not silently substituted for
the corpus that produced an older number.

Its null/golden/adversarial calibration triad tests the verifier before testing
an agent. An empty answer must fail, a synthesized correct answer must pass, and
a fluent dump that evades the structured contract must fail. Determinism and
container-parity checks were added after a broken baseline environment created
a retrieval advantage that disappeared when the control was repaired.

Later parity work found subtler asymmetries: wrong repository scope in one arm,
different revisions behind similar names, extra repository instructions loaded
from `AGENTS.md` or `CLAUDE.md`, and an extra scoreable output route available to
only one arm. Tests, not the write-up, enforced the correction by enumerating and
hashing the effective inputs reaching each arm.

CodeScaleBench also reports work-type and retrieval-quality breakdowns beside
the aggregate. Some suite intervals included zero and were reported as
indeterminate. A small pooled improvement did not support a claim that every
work type benefited.

**Transfer to this lab:** freeze the case suite and protocol, hash effective
configuration, compare matched run populations, validate environment parity,
and report results by failure boundary. A single cross-system winner would hide
the mechanism we are trying to study.

### EnterpriseBench: govern task mix, preserve provenance, and separate verifier failure

EnterpriseBench made corpus balance machine-enforced after a burst of valid but
similar tasks pushed one ecosystem from 40.7 percent to 46.2 percent during an
active balance correction. Each task could be defensible while the resulting
benchmark became less representative.

Its ground truth is anchored in merged changes, incidents, dependency bumps,
and postmortems where possible. Expected files carry provenance that separates
deterministic history from curator judgment. The task runner also distinguishes
an absent verifier verdict from a genuine agent score. A missing interpreter,
silent verifier exit, or broken environment is an infrastructure failure, not a
zero-quality result.

EnterpriseBench's run-promotion design came from another concrete failure: a
chain of publication scripts could leave official results partially promoted.
The replacement stages all artifacts, publishes with a same-filesystem rename,
updates its registry atomically, and retains a forensic failure snapshot.

**Transfer to this lab:** the first four durability cases are a purposive risk
set, not a representative corpus. Later expansion needs declared coverage gates
across identity, effects, cancellation, deployments, streams, and artifacts.
Every expected outcome needs provenance, and evidence publication must not turn
a half-written run into an official result.

### codeprobe: make adapters, scores, invalidity, and evidence explicit

codeprobe requires each adapter to declare the configuration controls it can
actually enforce. Unsupported controls fail preflight because silently dropping
one would make the experiment label false. It preserves partial output together
with the error and records whether cost or telemetry was measured, reported,
estimated, or unavailable.

Each score declares its scorer family. Outcome reward, diagnostic measures, and
optional model judgment remain separate. Its validity triage distinguishes a
valid result, a genuine evaluated failure, and an infrastructure failure. A run
with unresolved infrastructure casualties is retained but cannot be quoted;
those casualties neither disappear nor enter the reward mean as zeros.

For evidence that leaves the execution environment, codeprobe uses fixed schemas,
configuration and task digests, paired-set checks, minimum repeat gates,
owner-approved previews, atomic publication, and independent validation. Manual
evidence repair or raw-result reinterpretation disqualifies the comparison. Its
broader proof framework also supplies a useful claim rule: an explanation of a
failure remains a hypothesis until a distinguishing intervention reproduces it.

**Transfer to this lab:** adapters must fail closed on unsupported fault and
configuration controls; valid failure must stay distinct from an invalid trial;
the oracle must be independent of adapter logs; and every finding must cite a
machine-checkable evidence path.

### The resulting durability-benchmark policy

| Prior lesson | Rule for the Agent Durability Lab |
| --- | --- |
| Frozen CodeScaleBench suite versus maintained corpus | Every result names the contract version, case manifest, repository commit, adapter commit, system/SDK versions, binary hashes, OS, and effective configuration digest |
| Null/golden/adversarial verifier calibration | Each case must pass an unfaulted calibration, make the unsafe control fail in the predicted way, make the protected reference arm pass, and reject missed-boundary or tampered evidence as invalid |
| Hidden prompt and environment asymmetry | Inventory and hash environment, defaults, retry policy, timeouts, credentials, host limits, agent binary, destination, and oracle visibility for every arm |
| Paired and disaggregated analysis | Use the same case inputs and common workload across systems; report each failure boundary and safety track before any aggregate |
| EnterpriseBench task-mix drift | Declare which boundary classes the suite covers and limit conclusions to them; add coverage gates before calling a later suite representative |
| Ground-truth provenance | Tag every oracle predicate and guarantee as `system`, `application`, `destination`, `operating-system`, or a named combination |
| Verifier failure is not agent failure | Record `valid-pass`, `valid-fail`, and `invalid` separately; preserve all three, predeclare rerun rules, and never convert invalid evidence to a zero |
| codeprobe adapter capability honesty | Reject an adapter before launch if it cannot enforce the requested barrier, isolation, cancellation, timeout, or evidence-export contract |
| Scorer-family transparency | Keep correctness, recovery latency, durable bytes, calls, and operator intervention as separate measurements with named calculation methods |
| Evidence-bundle integrity | Stage evidence, validate its closed schema and cross-file bindings, publish append-only, and keep forensic artifacts from failed or invalid promotion |

The calibration rule is intentionally asymmetric. An unsafe control that passes
may mean the fault did not reach the dangerous window or the case cannot
distinguish the mechanism. It is not evidence that the durability system made
the application safe.

Repeated trials share a case and should not be treated as a broad task sample.
The planned 30 trials per arm estimate reliability for that exact case and
environment. Generalization across workload classes requires more cases and an
analysis that treats case or workload as the clustered unit.

## Remaining planned work

### Priority 1: cross-system evidence

The next program is specified by the
[cross-system benchmark contract](../../benchmarks/agent-durability/README.md).
It compares Temporal, Restate, DBOS Go, and a minimal PostgreSQL
queue/lease/outbox under the same four fault cases:

1. surviving external executor after the durability-system worker dies;
2. external effect succeeds before durable step completion is lost;
3. delayed effect, completion, and stop from a stale generation; and
4. cancellation while the exact process tree is unreachable.

The implementation order is:

1. Build the common simulator, authority store protocol, destination, barrier
   controller, evidence envelope, and independent oracle.
2. Add a Temporal adapter.
3. Add Restate, DBOS Go, and PostgreSQL adapters.
4. Run conformance tests before measuring anything.
5. Run at least 30 fresh trials per arm with randomized order, fixed host limits,
   retained invalid trials, and confidence intervals where supported.
6. Publish correctness first, then recovery latency, durable footprint,
   external-call amplification, operator intervention, and implementation
   surface among arms that reach output parity.

The repository tracks that sequence in Beads:

| Work | Bead | Dependency |
| --- | --- | --- |
| Common cross-system harness | `temporal_projects-y33.1` | Ready |
| Temporal adapter | `temporal_projects-y33.2` | Common harness |
| Restate adapter | `temporal_projects-y33.3` | Common harness |
| DBOS Go adapter | `temporal_projects-y33.4` | Common harness |
| PostgreSQL queue/lease/outbox adapter | `temporal_projects-y33.5` | Common harness |
| Final comparison and publication | `temporal_projects-y33.6` | All four adapters |

No system is currently declared conformant or superior. Native-minimum,
portable-safety, and optional native-optimized arms remain separate so an
application fence or co-transactional feature is not credited to the wrong
layer.

### Priority 2: unresolved Temporal boundaries

- **Worker Versioning and deployments:** test replay compatibility and the
  protocol contract between old external sessions and new Activity code
  (`temporal_projects-0xm`).
- **Workflow Streams:** record consumer-observed prefixes, duplicates, ordering,
  retry reconstruction, and durable cursor placement
  (`temporal_projects-cg5`).
- **Large artifacts and External Storage:** inject failure between artifact
  write, reference creation, reference persistence, Activity completion, and
  consumer acknowledgement (`temporal_projects-u93`).
- **Workflow versus Standalone Activity:** run the same workload with both and
  identify when preserved procedure adds value beyond durable execution
  (`temporal_projects-z8s`).
- **Heartbeat visibility:** distinguish a retry attached to a healthy surviving
  process from a phantom or wedged session (`temporal_projects-79z`).
- **Evidence authenticity:** replace the trusted loopback failure barrier with a
  stronger mechanism or explicitly bound what a barrier record proves
  (`temporal_projects-plu`).
- **Destination-enforced authority:** replace an owner while its old process
  retains copied credentials, then determine which destination capability or
  identity boundary rejects the stale mutation (`temporal_projects-k4x`).
- **Recurring-fault degradation:** inject several faults into one continuing run
  and compare recovery by fault ordinal with independent clean-start trials
  (`temporal_projects-uju`).

### Priority 3: operator diagnosis

Build an evidence-driven state model that distinguishes legitimate waiting,
retry exhaustion, live progress, a wedged agent, a missing Worker with a healthy
external process, stale authority, ambiguous effects, and lost application
progress despite a healthy Workflow (`temporal_projects-l7e`).

### Deferred until an experiment needs them

Nexus, Serverless Workers, CHASM internals, real model/framework integrations,
multi-host process supervision, cgroups, Durable Task, and AWS Step Functions
are not in the first comparison wave. They should enter when a current result
identifies a concrete decision their architecture could change.

## Limits and claims we should not make

- Gas City's Temporal agent-mutation path was not generally enabled. The
  evidence includes bounded canaries, failed promotion gates, and controlled
  failure tests. The later reviewed disposition was shadow/shadow and disarmed.
- The Gas City and current lab process-survival tests are single-host. They do
  not establish cross-host reattachment or hostile-process containment.
- File stores and fixture agents made several timing windows reproducible. They
  do not establish production database or model-provider behavior.
- The current comparison is a contract and work plan, not benchmark evidence.
- Temporal retry is not evidence of external exactly-once execution.
- A closed or progressing Workflow is not evidence that application work is
  correct, unique, live, or acknowledged.

## Source provenance

Current controlled findings are linked above and preserved with raw evidence in
this repository. The direct Gas City antecedents come from these internal field
records:

| Gas City record | Evidence used here |
| --- | --- |
| `docs/adr/0009-work-record-claim-lock-structured-outcome.md` | Four concurrent claims, divergent branch writers, and the claim/fence response |
| `docs/adr/0012-temporal-beads-orchestration-boundary.md` | Temporal, work-store, and Activity ownership split; wrong-boundary pilot; start-or-attach design |
| `docs/Temporal/README.md` | Pre-Temporal breakpoints, failed promotion gate, bounded canary history, and repair evidence |
| `docs/Temporal/Temporal_Technical_Design_QA.md` | Worker-kill test, stable identity before heartbeat, one-host limits, and completion/fencing design |
| `docs/design/temporal-decision.md` | Crash recovery motivation, retryable versus fail-closed modes, and the decision to retain cron for a small independent job |
| `docs/evidence/temporal-city-infra-handoff-2026-08-03.md` | Reviewed shadow/shadow state, invalid-history preservation, and authorization gate |
| `docs/evidence/outcome-worker-disposition-2026-08-04.md` | Final disarm ruling and correction of older continuous-production wording |

Where older Gas City status prose conflicts with the dated disposition record,
this brief uses the later reviewed disposition. That chronology is necessary to
keep a real canary lesson from becoming an unsupported production claim.

The benchmark-methodology antecedents come from these companion repository
records:

| Repository record | Method used here |
| --- | --- |
| CodeScaleBench `paper/sections/03-benchmark-construction.tex` | Frozen suites, pinned repository and container revisions, paired populations, oracle provenance, and designed sampling |
| CodeScaleBench `paper/sections/05-scoring-and-validation.tex` | Deterministic scoring, null/golden/adversarial verifier calibration, environment parity, and disaggregated reporting |
| CodeScaleBench `experiments/evaluator-benchmark/PROMPT_PARITY.md` | Mechanical audit of effective prompt, repository scope, hidden instructions, scoreable outputs, and configuration hashes |
| EnterpriseBench `docs/benchmark_design.md` | Machine-enforced task-mix and ecosystem-balance gates |
| EnterpriseBench `docs/TASK_AUTHORING_GUIDE.md` | Historical ground-truth provenance and the distinction between verifier infrastructure failure and a genuine zero |
| EnterpriseBench `docs/RUN_PROMOTION.md` | Staged atomic publication, rollback, registry consistency, and forensic failure snapshots |
| codeprobe `docs/scoring_model.md` | Explicit scorer families and separation of reward from diagnostics |
| codeprobe `docs/conventions/validity-triage.md` | Valid result, genuine failure, and infrastructure-failure populations with a non-quotable-run gate |
| codeprobe `docs/EVIDENCE_BUNDLE.md` | Fixed evidence schemas, digests, paired-set and repeat gates, atomic export, and independent validation |

The book-derived framework comes from these working-draft chapters in the
companion website repository:

| Book record | Principle used here |
| --- | --- |
| `src/content/book-chapters/engineering-reliable-coding-agents/introduction.md` | Evidence groups, dependency-chain reasoning, repair asymmetry, and separation of local artifacts from independent evidence |
| `src/content/book-chapters/engineering-reliable-coding-agents/isolation-injection-independent-verification.md` | Authority defines blast radius; recovery identities need an independent failure domain; completion must be verified against owned state |
| `src/content/book-chapters/engineering-reliable-coding-agents/persistent-state-durable-workflows-idempotent-retries.md` | Declared state, stable invocation identity, unknown-state reconciliation, and the boundary between durable procedure and external effects |
| `src/content/book-chapters/engineering-reliable-coding-agents/replayable-traces-fault-injection-recovery.md` | Typed causal traces, exact kill-point sweeps, negative controls, recurring faults, recovery event anchors, and claim envelopes |
| `src/content/book-chapters/engineering-reliable-coding-agents/human-auditable-failure-analysis-taxonomy.md` | First-upstream-failure attribution, evaluator checks, unresolved verdicts, and trace design for a skeptical reader |
| `src/content/book-chapters/engineering-reliable-coding-agents/execution-correction-gates-release-tests.md` | Independent execution evidence, retry versus correction, and candidate/environment/verifier failure separation |
| `src/content/book-chapters/engineering-reliable-coding-agents/cross-session-memory-raw-traces-compaction.md` | Immutable source events, rebuildable derived memory, schema provenance, and failure-driven compaction policy |
| `src/content/book-chapters/engineering-reliable-coding-agents/autonomy-provenance-gates-accountability.md` | Gates need causal power and reviewers need evidence, authority, and time to alter the consequential transition |
