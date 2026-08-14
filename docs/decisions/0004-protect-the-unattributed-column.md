# ADR 0004: Protect the unattributed column and the negative results

**Date:** 2026-08-14
**Status:** accepted
**Deciders:** Agent Durability Lab maintainers

## Context

The credibility of this corpus rests on the properties Temporal does not
supply. The guarantee summary's **No** cells, the unsafe controls that fail
first, and measurements that do not favor the durable-execution system are the
part a reader cannot get from vendor documentation.

That part is also the easiest to lose. Polishing a repository tends to convert
"Temporal does not provide X" into "Temporal plus your application together
provide X." Both sentences can be true; only the first tells the reader where
the responsibility sits when it breaks. An affiliation change, a submission to a
vendor catalog, or a well-meaning editing pass could each erode the distinction
without anyone deciding to.

## Decision

Four properties of this repository are not editorial preferences and do not
change with polish, packaging, or affiliation.

1. **The unattributed column stays unattributed.** A property Temporal does not
   supply is recorded as **No** with the mechanism that does supply it named
   separately. Merging the columns into a combined "together they provide"
   claim is prohibited, because it destroys the responsibility split that
   [ADR 0002](0002-separate-procedure-authority-and-effects.md) exists to keep.

2. **Unfavorable measurements stay, at full precision.** This specifically
   includes [Finding 0013](../findings/0013-application-policy-equalizes-safety-not-recovery-cost.md):
   protected median recovery of 45.5 ms for Temporal against 1 ms for
   PostgreSQL on the pinned host, alongside the finding that both fenced systems
   accepted zero obsolete actions. The number may be superseded by a fresh run
   set on stated hardware. It may not be dropped, rounded away, moved to a
   footnote, or replaced with a qualitative summary.

3. **The unsafe control is presented first.** Every finding, cookbook, and
   summary shows what breaks before it shows what holds. "Here is how Temporal
   solves X" is not an acceptable framing for a result whose evidence is "here
   is what happens without the application mechanism."

4. **Ownership and license.** The repository stays MIT under Stephanie Jarmak's
   name. If it ever moves to a vendor organization, including `temporalio`,
   the protections above must be written into the transfer agreement before the
   move, not after. Without that written protection, the transfer does not
   happen.

## Alternatives considered

### Rely on maintainer judgment

Individual judgment is what produced this structure, and it is also what an
affiliation change puts under pressure. A written record is what a future
maintainer, including a future version of the current one, can be held to.

### Soften the negative results once the mechanisms are productized

The cookbooks exist because the negative results are real. Removing the
negatives after shipping the patterns would leave a set of recommendations with
no evidence that anything goes wrong without them, which is the definition of a
pitch.

### Keep the protections private

A private intention is unenforceable by anyone else and invisible to a reader
deciding whether to trust the corpus. Publishing the constraint is part of the
evidence that it is being followed.

## Consequences

Reviews of external-facing prose check the **No** cells and the unsafe-first
ordering, not only accuracy. A pull request that reframes an unattributed
property as jointly provided is rejected on those grounds alone.

Revisit this ADR only to strengthen it, or when a fresh run set supersedes a
specific measurement. Supersession means a new number with its own evidence
root, published beside the old one. It never means deletion.
