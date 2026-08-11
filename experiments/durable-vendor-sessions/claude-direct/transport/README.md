# Claude direct evidence transport

The authenticated Claude evidence contains nested Git repositories and database
files. Adding those raw directories to the outer repository can create Git
entries with mode `160000`, while the repository-wide `*.db` rule can omit
required BoltDB and Temporal files. The local raw roots remain append-only and
unchanged. This transport makes their file artifacts cloneable without placing
any nested repository directly in the outer Git index.

## Format and invariant

[`transport-index.json`](../evidence-transport/transport-index.json) records the
ordered v1-v5 correction lineage and binds one archive and manifest per raw
root. Each manifest records every regular file's relative path, byte count,
permission bits, and SHA-256 digest. It also binds every finalized run's raw
inventory, declared raw-inventory hash, effective input, common manifest, and
verdict. The preserved incomplete v1 staging tree and root-level logs/databases
remain covered by the complete file inventory even though they are not
finalized runs.

Archives are deterministic `tar.gz` streams: entries are sorted, owner fields
and timestamps are normalized, modes are retained, and file bytes are streamed
from stable regular files. Source symlinks and special files are rejected.
Verification rejects unknown JSON fields, an unexpected package file, any hash
or inventory mismatch, unsafe archive paths, concatenated gzip streams, and
non-regular archive entries. Restore writes beneath a confined filesystem root,
refuses an existing destination, and re-inventories every restored bundle
before publishing it.

The invariant is file-artifact reconstruction: a clean clone can restore every
inventoried byte and file mode. Empty Git convenience directories are not raw
artifacts and are not packaged; reconstructed nested repositories pass
`git fsck --full`.

## Current package

The Git-safe package under `evidence-transport/` contains five roots, 2,206
files, 9,222,704 uncompressed bytes, 56 finalized run bindings, and the complete
v1-v5 correction chain. Its 11 transport files occupy 1,852,361 bytes. The
index SHA-256 is
`46a82476b4b47b103732121a10157434caefbc4661b2f7cc02cdce7df1714514`.

Two independent builds from the preserved originals produced identical package
files. A clean temporary Git repository staged the package with zero `160000`
entries, cloned it, restored all files and modes, and verified all 57 nested Git
repositories. The package does not change the admitted result: v1 is rejected,
v2-v4 are superseded correction lineage, and v5 remains the only admitted
suite.

## Resume-only package

The separate package under `resume-evidence-transport/` preserves the
caller-selected session/resume experiment without changing the unsafe-control
lineage above. It contains five roots, 1,759 files, 8,166,362 uncompressed
bytes, 44 finalized run bindings, and 11 transport files. Its index SHA-256 is
`107da44f12f0e9c9b6bd0a76095790e2943dd655c141d74d48ebfff779f838d3`.

Resume v1 and v4 are rejected partial runs. V2 and v3 completed but are
superseded because later review added raw invocation receipts and stricter
session-flag admission. V5 is the only admitted source-matched resume-only
suite. The ordered dispositions and explanations are bound by
[`claude-direct-resume-lineage.json`](claude-direct-resume-lineage.json).

Verify this package with:

```bash
go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/resume-evidence-transport
```

## Fenced-supervisor package

The protected correction lineage is packaged separately under
`fenced-evidence-transport-v2/`. It contains five roots, 1,697 files, 8,706,372
uncompressed bytes, 45 finalized run bindings, and 11 transport files. Its
index SHA-256 is
`d43a5463f0dcfd852744cbf52ca649f4898873985ea61a516c1438ce18f40c02`.

The authenticated v1 and hermetic v1 roots are rejected partial runs. Hermetic
v2 completed 15 runs but was superseded after review added the running-harness
hash and durable cancellation wait. Hermetic v3 also completed and passed, but
static-analysis and module-graph corrections changed the evidence-bound build
identities. Hermetic v4 is the admitted current-source suite. The dispositions
are bound by
[`claude-direct-fenced-lineage.json`](claude-direct-fenced-lineage.json).

The matched source-pinned resume-only control is independently packaged under
`resume-hermetic-evidence-transport-v2/`: two roots, 936 files, 3,399,329
uncompressed bytes, 24 finalized run bindings, and five transport files. Its
index SHA-256 is
`df92cbcf453e596f24a34ee1ea62ed2f8b8e5dd2899de2918a6ea68e147a7bb5`.
Control v1 is superseded and v2 is admitted; their dispositions are bound by
[`claude-direct-resume-hermetic-lineage.json`](claude-direct-resume-hermetic-lineage.json).

The earlier `fenced-evidence-transport/` and
`resume-hermetic-evidence-transport/` packages are retained as the pre-static-
analysis transport correction, not silently replaced.

The current-source authenticated comparison is preserved separately. The
`fenced-claude4-evidence-transport-v3/` package binds the rejected logged-out
root, superseded complete v1/v3, and admitted staticcheck-clean v4: four roots,
1,679 files, 8,593,538 uncompressed bytes, 45 finalized run bindings, and nine
transport files occupying 1,450,876 bytes. Its index SHA-256 is
`b3d2b35dc3f79038e9e968529a828b172fbd502b5eec657e940a4572a7481535`;
the ordered dispositions are bound by
[`claude-direct-fenced-claude4-lineage.json`](claude-direct-fenced-claude4-lineage.json).

The matched `resume-claude4-evidence-transport-v3/` package contains superseded
v1-v3 and admitted v4: four roots, 1,872 files, 7,856,513 uncompressed bytes,
48 finalized run bindings, and nine transport files occupying 1,604,386 bytes.
Its index SHA-256 is
`006e1d7544a34f4e1c123b2865a153e8300247249c9a26e64dcd4c480dd7e71a`;
its disposition is bound by
[`claude-direct-resume-claude4-lineage.json`](claude-direct-resume-claude4-lineage.json).
Two independent builds of each current authenticated package were byte-
identical. The earlier authenticated packages are retained as the pre-build-
graph/auditor/static-analysis corrections, not overwritten.

Verify both packages with:

```bash
go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/fenced-evidence-transport-v2

go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/resume-hermetic-evidence-transport-v2

go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/fenced-claude4-evidence-transport-v3

go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/resume-claude4-evidence-transport-v3
```

The semantic audit reports are written outside sealed evidence roots. After
restoring a package, rerun `claude-direct-evidence-audit` against the admitted
root to recompute verdicts, replay histories, and verify authority/effect
lineage without changing the restored root.

## Commands

Verify the clone-safe package:

```bash
go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  verify \
  --transport experiments/durable-vendor-sessions/claude-direct/evidence-transport
```

Restore it to a path that does not exist:

```bash
go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  restore \
  --transport experiments/durable-vendor-sessions/claude-direct/evidence-transport \
  --output /tmp/claude-direct-restored
```

To reproduce the package from the local append-only originals, choose a new
output path. The command refuses to overwrite an existing transport:

```bash
go run ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport \
  package \
  --source experiments/durable-vendor-sessions/claude-direct/evidence \
  --lineage experiments/durable-vendor-sessions/claude-direct/transport/claude-direct-lineage.json \
  --output /tmp/claude-direct-evidence-transport-rebuilt
```

Equivalent Make targets are `package-claude-direct-evidence`,
`verify-claude-direct-evidence`, and `restore-claude-direct-evidence`; they
require explicit transport and restore roots so an existing package is never
silently replaced.
