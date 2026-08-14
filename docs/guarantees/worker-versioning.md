# Worker deployment versioning

Versioning routes tasks. It does not decide whether a new build may attach to
an agent an old build created.

Back to the [guarantee summary](../guarantees.md).

## Detached-agent compatibility across Worker deployments

- **Temporal:** Worker Deployment Versioning routes pinned and auto-upgrade
  Workflow and Activity tasks and records deployment versions. It does not
  version an external agent protocol.
- **Your application:** record Worker and agent-build identity, declare
  compatible agent builds in each Activity implementation, and atomically reject
  incompatible attachment.
- **Your destination:** a durable session registry and a detached agent whose
  protocol actually matches the admitted build.
- **Evidence:** [three auto-upgrade and three pinned compatible trials attached the original `agent-v1`; three `worker-v3` trials rejected it without registry mutation](../findings/0022-worker-versioning-does-not-version-the-detached-agent-contract.md).
  All nine histories replayed, while the deliberately incompatible Workflow
  replay failed. Single host and simulated detached agent only.
