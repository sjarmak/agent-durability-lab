# Current Temporal surface relevant to the lab

Snapshot date: 2026-08-12. Product documentation is design input, not evidence of
application-level correctness.

| Capability | Current status | Research implication |
| --- | --- | --- |
| Worker Versioning | GA as of 2026-03-30 | Routes compatible Workflow/Activity tasks; does not revoke credentials or authority from an old detached agent process. |
| Standalone Activities | Public Preview; Server 1.31+ and CLI 1.7+ | Direct durable retry without a Workflow. The external-effect ambiguity remains, making this a useful later comparison for durable execution versus orchestration. |
| Workflow Streams | Public Preview | The Workflow-hosted log deduplicates a publisher ID/sequence, but Activity retries create fresh publishers and surface output from both attempts. [Finding 0023](findings/0023-workflow-stream-retries-need-output-reconstruction.md) observed process-buffer loss before flush, retained prefixes after flush, and the need for explicit retry reconstruction. |
| External Storage | Public Preview | Claim-check payload offload, not an artifact publication/acknowledgement protocol. Object-write/reference-history/orphan windows still need experiments. |
| Serverless Workers | AWS Lambda Public Preview; GCP Cloud Run Pre-release | Hard invocation lifetimes and ephemeral processes change which external agent-session patterns are viable. Not the first-milestone baseline. |
| Python OpenAI Agents SDK integration | Public Preview; used by the Temporal-reviewed Durable Agentic Harness sample | Makes model calls and selected tools visible as Activities inside a Workflow. It is a useful native baseline, but its Worker-kill demo does not establish external-process ownership, effect deduplication, client-start idempotency, or durable UI delivery. |
| Sandbox Orchestration Harness | Community Code Exchange sample; Go source reviewed and live-tested at `e8a8854` | Provides a child-Workflow lifecycle, stable Update IDs, attachable references, explicit cleanup, suspend/resume, and snapshot/fork provider capabilities. The lab observed that provider calls still need effect-level idempotency, attached writers need destination-enforced fencing, and pre-status creation needs provider reconciliation. |
| Nexus | Core Nexus GA; Standalone Nexus Operations remain less mature | Stable Nexus start request IDs can deduplicate starts, not arbitrary downstream effects. |
| CHASM | Internal server architecture, not a public Go application primitive | Versioned transitions and component references are useful analogies for fencing; the lab must not couple application correctness to CHASM internals. |

Primary references:

- [Worker Versioning GA announcement](https://temporal.io/changelog/worker-versioning-continue-as-new-worker-controller)
- [Standalone Activities](https://docs.temporal.io/standalone-activity)
- [Workflow Streams](https://docs.temporal.io/workflow-streams)
- [External Storage](https://docs.temporal.io/external-storage)
- [Serverless Workers](https://docs.temporal.io/serverless-workers)
- [Python SDK OpenAI Agents integration](https://github.com/temporalio/sdk-python)
- [Durable Agentic Harness](https://temporal.io/code-exchange/durable-agentic-harness)
- [Durable Agentic Harness source](https://github.com/temporal-sa/durable-agentic-harness/tree/4afef65defcd8e70d6e794936320e4d7513fd365)
- [Sandbox Orchestration Harness blog](https://temporal.io/blog/temporal-sandbox-orchestration-harness-the-missing-layer-for-running-agents)
- [Sandbox Orchestration Harness source](https://github.com/temporal-community/sandbox-orchestration-harness/tree/e8a88540d9523a3d9070860913567670194bacc1)
- [Nexus](https://docs.temporal.io/nexus)
- [CHASM architecture](https://github.com/temporalio/temporal/blob/v1.31.2/docs/architecture/chasm.md)
- [Go SDK 1.47.0 release](https://github.com/temporalio/sdk-go/releases/tag/v1.47.0)

The completion-identity boundary is now live-tested on the versions above.
Normal Activity task-token completion rejected attempt 1 after attempt 2
started. `CompleteActivityByID` accepted a result attributed to attempt 1 and
completed the current logical Activity. See
[finding 0003](findings/0003-activity-id-completion-is-not-attempt-scoped.md).

The documented Activity crash window is also now reproduced directly: a Worker
was killed after each of six destination classes confirmed an effect and before
the Activity returned. Temporal retried and recorded one Activity completion,
while every unsafe destination recorded two effects. See
[finding 0004](findings/0004-one-temporal-completion-can-hide-two-effects.md) and
Temporal's [Activity idempotency guidance](https://docs.temporal.io/activity-definition#idempotency).

The pinned Sandbox Orchestration Harness is now calibrated through 36 live
trials. Stable outer Update IDs did not deduplicate retried inner provider
effects; opaque references did not revoke stale writers; and parent close could
not clean a created resource whose status never reached Workflow state. Atomic
provider receipts, generation fencing, and an external reconciler closed those
bounded gaps in the hermetic arms. See
[finding 0009](findings/0009-sandbox-lifecycle-does-not-close-provider-gaps.md).
