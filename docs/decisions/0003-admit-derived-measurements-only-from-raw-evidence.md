# ADR 0003: Admit derived measurements only from raw evidence

**Date:** 2026-08-09
**Status:** accepted
**Deciders:** Agent Durability Lab maintainers

## Context

The topology harness records latency, retry, load, cost, history, and outcome
metrics beside causal events and external-service observations. Several early
checks validated that the metric names and values were internally plausible but
did not independently derive every value. A faulty arm or generator could
therefore report a favorable metric while leaving the raw trace unchanged.

## Decision

A derived field may affect admission or a supported claim only when the
independent oracle can reconstruct it from sealed raw evidence. The raw boundary
includes causal events, dependency requests, destination actions, per-item
lifecycle observations, process identity records, and parsed native system
history. Event counts and byte counts come from the native export itself.
Registered windows and aggregation rules are constants in the frozen protocol,
not choices made by an executor.

Synthetic histories remain valid for deterministic fixture controls only when
they explicitly declare fixture provenance. A live run must contain parseable
native history and must fail closed if fixture provenance is present. If a
metric cannot yet be reconstructed, it may be retained as diagnostic telemetry
but cannot determine admission or a comparative result.

## Alternatives considered

### Trust the common generator

Both topology arms use the same generator, which reduces differential bias but
does not prevent a common measurement defect. This was rejected because the
benchmark's purpose is to detect common orchestration failure modes as well as
arm differences.

### Validate only aggregate reports

Aggregate arithmetic is useful for a quick audit but can preserve a bad
per-run value consistently. This was rejected because the raw episode is the
smallest independently falsifiable unit.

### Recompute metrics during evidence generation

Generation-time recomputation catches implementation mistakes but still asks
the producer to validate itself. This was rejected as the only check; the
disk-only oracle must be able to repeat the derivation later.

## Consequences

The evidence format must retain enough raw detail to reproduce every claimed
quantity, and metric additions require a reconstruction rule plus a mutation
test. Canonical history encoding is part of byte accounting. Oracle code grows
with the registered metric surface, but unsupported or synthetic measurements
can no longer silently enter efficiency comparisons.

This decision should be revisited if a metric is inherently unavailable from
the tested systems. The acceptable outcome is to mark that quantity unresolved
or add an independently observed source, not to weaken admission silently.
