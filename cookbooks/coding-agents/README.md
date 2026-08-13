# Fault-Tested Durability Patterns for Coding Agents

These six recipes are the executable product surface for the
[coding-agent durability v1 specification](../../docs/product/coding-agent-durability-v1.md).
They turn admitted experiment outputs into patterns a backend/platform engineer
can apply without treating a passing demo as a guarantee.

The [product brief](../../docs/product/fault-tested-coding-agent-cookbooks.md)
defines the user journey and claim boundary. The
[presentation contract](presentation/README.md) defines the read-only view that
future tutorials and the evidence explorer consume.

## Start

Run the first trustworthy recovery walkthrough. It needs Go, but no provider
credentials:

```bash
./cookbooks/coding-agents/quickstart.sh
```

The command requires one unfaulted pass, the unsafe distinguishing failure, the
matching protected pass, independent audits, and all 102 native history replays
before it renders a summary. See the [quickstart contract](quickstart/README.md).

Explore the same verified triad interactively, with normalized timeline navigation beside
native history, raw evidence, authority, effects, provenance, and falsifier:

```bash
./cookbooks/coding-agents/explore.sh
```

See the [read-only explorer contract](explorer/README.md).

For the short failure-first learning path, start with the
[tutorials](tutorials/README.md).

For a pinned Codespaces or VS Code Dev Containers workspace and its focused smoke gate,
see the [development workspace](../../.devcontainer/README.md).

The broader read-only fresh-checkout cookbook audit is:

```bash
./cookbooks/coding-agents/run-all.sh check
```

Run all focused critical paths, including history replay and mechanism tests:

```bash
./cookbooks/coding-agents/run-all.sh critical
```

Run both phases with:

```bash
./cookbooks/coding-agents/run-all.sh all
```

The audit reads admitted evidence in place. Live evidence generation remains in
each underlying experiment and always requires a new append-only root.

## Patterns

The supporting contract is language-neutral:

- [v1 protocol and fixtures](../../specs/coding-agent-durability/v1/README.md)
- [incubating Go binding](../../contrib/codingagent/go)
- [incubating Python binding](../../contrib/codingagent/python)
- [deterministic apparatus conformance](../../benchmarks/agent-durability/conformance)

### Cookbook map

| Recipe | Product decision it supports | Maturity |
| --- | --- | --- |
| [01-native-agent-loop](01-native-agent-loop/README.md) | Put deterministic agent orchestration in Workflow code; isolate model/tool calls in Activities; replay completed steps; fence ambiguous effects | Normative at the pinned OpenAI Agents exemplar; Continue-As-New stream transfer experimental |
| [02-effect-safe-tools](02-effect-safe-tools/README.md) | Select a destination-specific idempotency, transaction, or reconciliation contract for six tool classes | Normative for the tested destination boundaries |
| [03-external-cli-ownership](03-external-cli-ownership/README.md) | Reject direct relaunch and resume-only as ownership; use an external start-or-attach supervisor with generation/capability fencing | Normative for tested single-host Claude/Codex CLI boundaries; provider/version compatibility remains bounded |
| [04-cancellation-and-cleanup](04-cancellation-and-cleanup/README.md) | Commit application revocation before exact stop delivery, acknowledgement, and process-tree verification | Normative for the tested single-host boundary |
| [05-sandbox-lifecycle](05-sandbox-lifecycle/README.md) | Separate sandbox operation identity, resource ownership, agent session identity, and orphan reconciliation | Normative for the hermetic provider boundary |
| [06-bounded-recovery](06-bounded-recovery/README.md) | Own retry budgets, admission, catch-up, backpressure, poison quarantine, and progress deadlines in Workflow/application state | Normative mechanism conformance; not a performance ranking |

### Universal pattern

Temporal supplies durable procedure: Workflow state, timers, task redelivery,
cancellation commands, and Event History. The application supplies stable
session/turn/operation/effect identity, one current authority generation and
capability, start-or-attach decisions, explicit terminal state, recovery policy,
and independent evidence. The destination supplies atomic fencing,
idempotency/uniqueness or reconciliation, and durable lookup receipts.

The portable sequence is:

1. Mint stable logical identities before scheduling external work.
2. Claim one generation/capability and durably record launch intent.
3. Start or attach the exact executor; never infer identity from Activity
   attempt, task token, PID, or transcript ID.
4. Require every authoritative effect and result to compare current authority
   and stable content identity at the destination boundary.
5. Reconcile a lost Activity completion from the destination receipt.
6. On cancellation or replacement, commit revocation/fencing first; then stop
   the exact executor and record delivery, acknowledgement, and disposition.
7. Bound retries, admission, catch-up, poison, and progress using deterministic
   Workflow procedure.
8. Preserve exact barriers, unsafe controls, raw observations, histories, and
   independent verdicts.

There is no exactly-once wrapper here: the rule is no exactly-once claim without
a destination protocol and evidence that establishes effect cardinality.

## Scenarios

Every public scenario is an exact triad: an unfaulted valid-pass, an unsafe
valid-fail negative control, and a protected valid-pass under the same declared
fault. The scenario retains stable logical identity, delivery/process/provider
observations, authority changes, destination effects, cancellation chronology,
and the terminal outcome. The typed contract lives in
[`presentation`](presentation/README.md).

## Evidence

The normalized trace is a navigation aid over independently verified evidence.
Native Temporal history, raw destination or workspace observations, hashes,
source/version pins, replay status, and correction lineage remain available.
The presentation layer cannot choose a verdict or hide a failed or superseded
population.

## Protocol

Portable identities, transitions, events, receipts, fixtures, and binding
requirements live under
[`specs/coding-agent-durability/v1`](../../specs/coding-agent-durability/v1/README.md).
Go and Python bindings consume that contract; cookbook-specific mechanisms do
not replace it.

## Research

This package establishes evidence-backed design patterns and executable
mechanism checks. The shared external-CLI ownership contract is normative at
the admitted single-host Claude Code and Codex CLI boundaries; it is not a
general provider or version compatibility promise. Provider authentication
lifecycle, cross-host supervisor failover, arbitrary real destinations,
version migration, protocol-native APIs, and public performance or failure-rate
claims remain outside v1.

The [guarantee ledger](../../docs/guarantees.md) and
[findings](../../docs/findings/) are the authoritative claim index. Product
presentation remains downstream of those records.
