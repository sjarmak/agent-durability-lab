# Fault-tested coding-agent tutorials

This learning path is for backend and platform engineers who need the whole agent
application—not only the Workflow—to remain correct after Temporal recovery. It starts with a
visible failure, adds the smallest protected mechanism, then keeps the native history and raw
evidence beside the conclusion.

## 1. First trustworthy recovery

Run the credential-free audit and summary:

```bash
./cookbooks/coding-agents/quickstart.sh
```

The result is an exact triad: an unfaulted pass, an Unsafe distinguishing failure, and the
matching Protected pass. Continue with [Inspect a recovery](01-inspect-a-recovery.md) to compare
stable identity, delivery attempts, authority, destination effects, and the native history.

## 2. Apply the universal pattern

Read [Apply the pattern](02-apply-the-pattern.md) before adapting a recipe. The portable sequence
is: register stable identity; authenticate the current owner; start-or-attach; publish effects
through a destination capability; fence before replacement; revoke before stop; and complete only
from the current generation.

## 3. Read evidence without overclaiming

Open the loopback-only explorer:

```bash
./cookbooks/coding-agents/explore.sh
```

Use [Read the evidence](03-read-the-evidence.md) to keep normalized explanation separate from the
independent oracle. The walkthrough ends with the responsibility split and falsifier, not a generic
provider, exactly-once, cross-host, or performance claim.

## Verify the packaged surface

Audit every cookbook from a clean working directory:

```bash
./cookbooks/coding-agents/run-all.sh check
```

Run the same credential-free product gate used by CI:

```bash
./cookbooks/coding-agents/dev-smoke.sh
```

The gate exercises the quickstart, the explorer launch and shutdown path, presentation contracts,
independent admitted-evidence reconstruction, native history replay, and focused race tests.
