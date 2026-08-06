# ADR 0001: Evidence before shared abstraction

**Status:** accepted

## Context

The lab will study many Temporal and agent failure boundaries, which creates an
early temptation to build a generic agent framework, storage interface, or fault
platform. Those abstractions can erase the precise behavior under study.

## Decision

An experiment starts with its invariant, negative control, deterministic barrier,
and oracle. A mechanism remains local until a second experiment needs the same
boundary. Shared `internal/` packages must represent evidenced cross-experiment
responsibilities such as ownership fencing or event recording, not predicted
future reuse.

## Consequences

Some early code will be intentionally narrow. Duplication may exist briefly when
two experiments need to prove that their semantics are actually the same. The lab
accepts this cost to keep controls legible and conclusions attributable.
