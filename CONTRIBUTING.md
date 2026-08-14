# Contributing

This is an evidence lab. The bar for a change is what would have caught it if it
were wrong.

## Ways in, easiest first

1. **Falsify something.** Every finding ends with what would change its
   conclusion. If you produce that result on your hardware, your versions, or
   your destination, open an issue with the raw run directory. A finding that
   survives a real attempt is worth more than one that was never attacked.
2. **Report a scope error.** If a claim reads wider than its evidence supports,
   say which sentence and which run set. Overclaiming is the failure mode this
   repository most wants reported.
3. **Add a destination.** [Finding 0004](docs/findings/0004-one-temporal-completion-can-hide-two-effects.md)
   covers six destination classes. Real brokers, object stores, and hosted Git
   are untested. A new destination needs an unsafe control that actually
   duplicates and a protected arm that does not.
4. **Port a pattern.** The [protocol v1](specs/coding-agent-durability/v1/README.md)
   has Go and Python bindings under [`contrib/codingagent`](contrib/codingagent).
   Other languages are welcome if they pass the shared fixtures.
5. **Add an experiment.** The heaviest path. Read
   [AGENTS.md](AGENTS.md) and the
   [experiment methodology](docs/experiment-methodology.md) first.

## What a new experiment needs

Write the contract before the harness. It states the invariant, the exact
failure boundary, the machine-checkable oracle, the identities, the
responsibility split among durable system, application, and destination, and
what would falsify the result.

Then the run has to earn its claim:

- an **unsafe negative control** capable of violating the invariant, and which
  actually does;
- faults injected at **named barriers**, never `time.Sleep` or a timing guess;
- **repeated trials** for anything concurrency-sensitive;
- **raw evidence preserved** in an append-only root, including runs that failed
  because the harness was wrong;
- an **independent oracle** that reads raw records, not adapter logs.

A passing demo is not evidence, and neither is a single green run. If the unsafe
arm also passes, the experiment has not shown anything yet.

## Rules that are not negotiable

- **Never delete, rewrite, or hide failing raw evidence.** If a harness was
  wrong, keep the original result and add the corrected run beside it with the
  explanation. The correction lineage is part of the record.
- **Never widen a claim past its run set.** Say which guarantee came from
  Temporal, which from application code, and which from the destination.
- **Do not write "exactly once"** unless the destination protocol and the
  evidence establish it. Temporal Activity retries alone do not make effects
  exactly once.
- **Workflow code stays deterministic.** No direct IO, subprocesses, native
  goroutines, wall-clock time, randomness, or unordered map decisions. Replay
  compatibility is an acceptance criterion, not a nice-to-have.
- **Tests ship in the same commit as the fix.**

## Before you open a pull request

```bash
make build
go test -race ./...
make coverage
```

Live experiments start local services and signal real subprocesses. Read the
experiment README first. Evidence commands require an explicit output root and
never overwrite an existing run directory.

Findings follow [the finding template](docs/findings/TEMPLATE.md): abstract,
numbers, claim, method, then a single scope block carrying the caveats.

## Scope this repository will decline

No UI, no generic agent framework, no deployment platform. A mechanism lives
beside its experiment until a second experiment needs it. Product-like code
belongs here only when an experiment requires it or repeated evidence justifies
reuse.

## Issues and discussion

Open an issue at
<https://github.com/sjarmak/agent-durability-lab/issues>. For a falsification
report, include the versions, the host, the exact barrier, and the run
directory. Licensed MIT; contributions are accepted under the same license.
