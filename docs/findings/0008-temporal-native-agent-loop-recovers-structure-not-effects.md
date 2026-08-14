# Finding 0008: A Temporal-native agent loop recovers structure, not effects

An OpenAI Agents loop inside a Temporal Workflow made model calls, tool calls,
approvals, and stream state replayable. Replay restored completed results.
Three unsafe tool trials still applied the same effect twice.

**Status:** observed in six common-oracle live trials and exact-boundary
integration tests

**Versions:** Temporal Server `1.31.2`; local test-server CLI `1.8.2`; Temporal
Python SDK `1.31.0`; OpenAI Agents SDK `0.19.4`; Python `3.12.3`; Linux amd64

**Source identities:** Python implementation
`872f40f05fb8a8ceb6be29f45121145abb37c8cab9a5688f3c456d34bc04f616`;
Go evidence adapter
`e883480d80d9fff950bb2b4731c89baa0a60da210a6ad6a93a36449784d1c57b`

## Claim

Putting the OpenAI Agents SDK loop inside a Temporal Workflow makes the model
calls, tool calls, approval state, typed result, stream state, and
Continue-As-New transition visible to Temporal recovery. Replay after Worker
death restores completed model and tool Activity results rather than rerunning
them. It does not make a tool's external effect exactly once.

At the `tool-effect-committed` boundary, all three unsafe trials delivered the
same logical effect twice and SQLite applied it twice. All three protected
trials also recorded two physical deliveries, but a stable logical effect ID and
destination transaction applied it once. The independent common oracle scored
the unsafe runs `valid-fail / duplicate_physical_effect` and protected runs
`valid-pass`; no run was invalid.

## Other observed boundaries

- When a successful StartWorkflow response was withheld from the application,
  retrying the same Workflow ID with `USE_EXISTING` recovered the same run and
  produced one effect.
- Killing the Worker after the deterministic model constructed its response
  retried only that incomplete streaming model Activity. The tool ran once.
- Killing the Worker after the Workflow published `result_built` scheduled no
  additional model or tool Activity. A replacement Worker replayed and waited
  for the release Signal before cleanup and completion.
- Workflow cancellation while awaiting approval ran a retry-safe cleanup
  Activity. Termination is the negative control: it ended the Workflow without
  executing Workflow cleanup.
- A WorkflowStream subscriber followed Continue-As-New, received the pre- and
  post-transition event sequence, and observed one unchanged session owner
  capability. The approval gate remained closed in the successor run.
- Replaying a captured completed history succeeded when the replayer used the
  same streaming plugin parameters. Omitting those parameters was correctly
  rejected as nondeterministic during development.

## Responsibility split

- Temporal durably records Workflow decisions, Signals, Activity results,
  retries, and run transitions. The OpenAI Agents plugin maps model calls and
  agent tools onto those primitives.
- Application code mints stable session, turn, effect, generation, and owner
  identities; correlates typed results; carries identity and stream state across
  Continue-As-New; chooses start-conflict and cancellation policy; and performs
  retry-safe cleanup.
- The destination supplies effect deduplication. Temporal Activity retry is
  delivery recovery, not destination atomicity.
- Workflow cancellation permits cooperative cleanup; termination deliberately
  bypasses Workflow code and therefore cannot promise it.

## Scope — what this does not show

The model is deterministic and hermetic; no OpenAI network response or token
stream is claimed. The controlled destination is local SQLite. The process
faults are single-host. The stream test publishes serializable agent lifecycle
items and one terminal model event; it does not establish consumer
acknowledgement, retention after Workflow close, or exactly-once UI delivery.
Continue-As-New was forced at one controlled boundary rather than after a
production-sized history. Three trials per effect arm establish repeatability,
not a failure-rate estimate.

Only the ambiguous-effect matrix is admitted to the append-only common oracle.
The other boundary observations are exact integration tests and should not be
read as cross-system benchmark results.

## Evidence and what would change this conclusion

The evidence is
[`temporal-native-20260807-v3`](../../experiments/durable-vendor-sessions/temporal-native/evidence/temporal-native-20260807-v3).
Every run contains full Temporal Event History, barrier arrival, process
controls, destination snapshot, common events, input and source hashes, and the
independent verdict.

V1 and v2 are preserved but superseded. They produced the same verdicts, but
review found that v1's agent hash covered only `worker.py` and its adapter
identity was a semantic version. V2 corrected the hash scope; a subsequent
source refactor required v3 so the admitted evidence matches the handed-off
Python package. No earlier evidence file was deleted or rewritten.

The claim is falsified if replay reissues a completed model or tool Activity, a
continued run changes session ownership or loses its approval/stream state, a
protected trial applies the logical effect twice, an unsafe trial stops exposing
the duplicate, cancellation is reported as cleanup after termination, the
fault is not bracketed by the first and second destination observations, or a
fresh source-pinned three-trial suite changes the stated verdicts.
