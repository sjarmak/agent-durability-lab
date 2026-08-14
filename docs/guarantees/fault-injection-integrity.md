# Fault-injection integrity

This page is about the instrument, not the system under test. A fault barrier
that can be triggered by the wrong caller is not an exact barrier.

Back to the [guarantee summary](../guarantees.md).

## Authenticated local fault boundary

- **Temporal:** no.
- **Your application:** pre-register the exact point, session, generation, and
  actor tuple. Authenticate the arrival body with a random per-run credential and
  single-use nonce before changing barrier state. Pass the credential through an
  inherited descriptor and exclude it from portable evidence.
- **Your destination:** a trusted local controller and process or descriptor
  isolation. This is fault-selection integrity, not external-agent authority.
- **Evidence:** `internal/failureinject` actively rejects wrong credentials,
  identity substitutions, arrival-ID reuse, and nonce replay without advancing
  the barrier. The Codex launcher integration binds its process receipt to the
  authenticated arrival and scans produced artifacts for the credential canary.
  Previously admitted run sets remain historical evidence from the earlier
  trusted-loopback protocol.
