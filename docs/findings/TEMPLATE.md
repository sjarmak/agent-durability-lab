# Finding NNNN: <claim as a sentence, not a topic>

<Abstract: at most forty words, no subordinate clauses. What broke, what fixed
it, how many trials. A reader who stops here should be able to repeat the
sentence to a colleague correctly.>

**Status:** observed in <N> valid trials
**Versions:** <pinned versions and platform>
**Source identities:** <binary and harness digests, when the run set pins them>

## Numbers

State counts before method. Put the unsafe control first.

| Arm | Trials | Result |
| --- | --- | --- |
| Unsafe control | N | what failed, in the oracle's terms |
| Protected | N | what held |
| Unfaulted | N | calibration |

## Claim

The full claim, including what it narrows or supersedes in earlier findings.

## Failure boundary and oracle

The exact barrier, the component disrupted, the recorded ordering, and the
independent check. Barriers are named events, never timing guesses.

## Observations

Per-arm detail with links to raw evidence. Preserved invalid or superseded runs
belong here too, with why they were rejected. Nothing is deleted.

## Responsibility split

- **Temporal:** what the durable execution system did.
- **Application:** what the application had to own.
- **Destination:** what the external system had to enforce.
- **Harness:** what the controller supplied. These are evidence mechanisms, not
  production guarantees.

## Scope — what this does not show

Every caveat that would otherwise be scattered through the prose above belongs
here as a list. Versions not tested, concurrency not exercised, hosts not
crossed, destinations not real, frequencies not established.

## What would change this conclusion

The observable result that would narrow or overturn the claim, stated so that
someone else could go and produce it.
