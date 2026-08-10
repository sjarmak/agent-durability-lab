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
