# Current Temporal surface relevant to the lab

Snapshot date: 2026-08-06. Product documentation is design input, not evidence of
application-level correctness.

| Capability | Current status | Research implication |
| --- | --- | --- |
| Worker Versioning | GA as of 2026-03-30 | Routes compatible Workflow/Activity tasks; does not revoke credentials or authority from an old detached agent process. |
| Standalone Activities | Public Preview; Server 1.31+ and CLI 1.7+ | Direct durable retry without a Workflow. The external-effect ambiguity remains, making this a useful later comparison for durable execution versus orchestration. |
| Workflow Streams | Public Preview | The Workflow-hosted log deduplicates a publisher ID/sequence, but Activity retries create fresh publishers and surface output from both attempts. Process-buffered events can be lost. |
| External Storage | Public Preview | Claim-check payload offload, not an artifact publication/acknowledgement protocol. Object-write/reference-history/orphan windows still need experiments. |
| Serverless Workers | AWS Lambda Public Preview; GCP Cloud Run Pre-release | Hard invocation lifetimes and ephemeral processes change which external agent-session patterns are viable. Not the first-milestone baseline. |
| Nexus | Core Nexus GA; Standalone Nexus Operations remain less mature | Stable Nexus start request IDs can deduplicate starts, not arbitrary downstream effects. |
| CHASM | Internal server architecture, not a public Go application primitive | Versioned transitions and component references are useful analogies for fencing; the lab must not couple application correctness to CHASM internals. |

Primary references:

- [Worker Versioning GA announcement](https://temporal.io/changelog/worker-versioning-continue-as-new-worker-controller)
- [Standalone Activities](https://docs.temporal.io/standalone-activity)
- [Workflow Streams](https://docs.temporal.io/workflow-streams)
- [External Storage](https://docs.temporal.io/external-storage)
- [Serverless Workers](https://docs.temporal.io/serverless-workers)
- [Nexus](https://docs.temporal.io/nexus)
- [CHASM architecture](https://github.com/temporalio/temporal/blob/v1.31.2/docs/architecture/chasm.md)
- [Go SDK 1.47.0 release](https://github.com/temporalio/sdk-go/releases/tag/v1.47.0)

The completion-identity boundary is now live-tested on the versions above.
Normal Activity task-token completion rejected attempt 1 after attempt 2
started. `CompleteActivityByID` accepted a result attributed to attempt 1 and
completed the current logical Activity. See
[finding 0003](findings/0003-activity-id-completion-is-not-attempt-scoped.md).
