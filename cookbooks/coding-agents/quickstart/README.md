# First trustworthy recovery quickstart

## Question

Can a fresh-checkout user see a real unsafe recovery fail and the matching
application-fenced recovery pass without provider credentials?

## Invariant

One logical operation produces at most one accepted destination effect. A
Temporal Activity retry is delivery, not permission to start a competing agent
or repeat the effect.

## Failure boundary

The selected fault is the exact `codex-tool-effect-committed` barrier. The tool
effect is durable, Activity completion is still absent, and the Worker is then
replaced. The unsafe arm starts fresh; the protected arm attaches to the current
application-owned executor.

## Oracle

The command runs the existing independent Codex transport audits under the Go
race detector. It restores sealed bundles, checks exact inventory and hashes,
reconstructs every verdict from raw evidence, and replays every captured
Temporal history. The command requires exact pass receipts for the top-level
audit and all six hermetic/authenticated subtests; a missing, skipped, or failed
case stops before presentation.

`catalog.json` is a read-only projection of three admitted v12 trials. It is not
an oracle and it is not new evidence. Archive-member links point from the
normalized view back to each trial summary and native Workflow history.

## Run

From any working directory:

```bash
/path/to/temporal_projects/cookbooks/coding-agents/quickstart.sh
```

From the repository root:

```bash
./cookbooks/coding-agents/quickstart.sh
```

The command accepts no scenario filter. This prevents a partial run from
silently omitting the unsafe negative control or protected case.

## Evidence

The audit reconstructs the six final v12 Codex transports and replays all 102
histories. The concise output selects:

- one unfaulted fenced valid-pass with one physical effect;
- one unsafe effect-before-completion valid-fail with two physical effects; and
- the matching protected valid-pass with one physical effect and exact
  start-or-attach.

The catalog links the transport archives, exact members, manifests, independent
reports, and append-only correction indexes. Older populations remain visible
in those indexes.

## Responsibility split

- Temporal records the procedure and redelivers the incomplete Activity.
- The application owns stable logical identity, generation/capability
  authority, and exact start-or-attach.
- The destination accepts only a current authorized effect and preserves its
  receipt.
- The executor supplies process/thread observations; neither is durable turn
  authority.

## Falsifier

Reject or narrow the walkthrough if the unsafe arm stops distinguishing, the
protected arm commits twice or launches a competitor, any selected history
fails replay, an exact audit receipt is absent, or the presentation disagrees
with its raw trial summary.

This is a credential-free audit of preserved single-host evidence, not a fresh
live provider experiment. It does not establish current provider compatibility,
cross-host recovery, performance, credential durability, or exactly-once
effects.
