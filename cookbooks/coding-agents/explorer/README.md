# Recovery evidence explorer

## Question

Can an engineer understand one unsafe recovery and its protected counterpart without losing
the exact identities, authority, raw evidence, native history, responsibility boundary, or
falsifier that make the conclusion trustworthy?

## Invariant

The explorer is a read-only surface over the embedded, validated presentation catalog. Every raw or native
artifact request uses an opaque catalog selector, re-verifies the exact sealed transport,
checks the catalog and manifest digests, and returns one bounded regular file or archive
member. Presentation cannot create, alter, admit, or rescore evidence.

## Failure boundary

The default scenario is the exact `codex-tool-effect-committed` boundary: the destination
effect is durable, Activity completion is absent, and the Worker is replaced. The unsafe
fresh launch commits the logical effect twice. The application-fenced retry attaches to the
authorized executor and retains one effect.

## Oracle

The independent Codex disk auditor remains the oracle. The credential-free quickstart runs
that audit and replays all 102 native histories before presentation. The explorer separately
checks transport inventory, hashes, manifest membership, regular-file type, symlink absence,
and read bounds every time a user opens a raw record. The normalized timeline is explanatory,
not the oracle.

## Run

From any working directory, run:

```bash
/path/to/temporal_projects/cookbooks/coding-agents/explore.sh
```

Then open `http://127.0.0.1:8080`. The server accepts no non-loopback listen address. In the
pinned Dev Container or Codespaces, port 8080 is forwarded on demand. Stop with `Ctrl-C`;
the server performs a bounded graceful shutdown and owns no background service or mutable
state.

## Evidence

The UI keeps the unfaulted, unsafe, and protected v12 episodes together. It shows the same
effect boundary and logical identity, physical effect count, authority record, process and
delivery observations, normalized sequence, direct native-history/raw-record responses,
manifest, independent audit, and correction lineage. Artifact paths are metadata and are
never executed, fetched as repositories, or exposed as filesystem URLs.

## Responsibility split

- Temporal records and redelivers procedure.
- The application owns stable identity, authority, and start-or-attach policy.
- The destination enforces the authorized effect and preserves its receipt.
- The executor supplies process and provider observations, not durable authority.
- The explorer renders records and makes no guarantee of its own.

## Falsifier

Reject the surface if it hides the unsafe control, serves bytes that differ from the sealed
transport or manifest, follows an unverified path, treats normalized events as the oracle,
or makes the protected and raw observations disagree. The scenario remains bounded to the
pinned single-host hermetic evidence. It is not a provider compatibility, cross-host,
performance, credential durability, or exactly-once claim.
