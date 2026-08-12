# Direct Codex CLI recovery controls

**Status:** observed in 51 source-pinned v12 hermetic and 51 matched
authenticated Codex CLI trials; the current tree includes later authenticated-
barrier hardening
**Tracking:** `temporal_projects-5im.9`
**Pinned vendor surface:** Codex CLI `0.147.0`, model `gpt-5.6-sol`, reasoning
effort `low`, `workspace-write` sandbox, fixed authenticated `codex-2` profile

## Question

What survives when a retryable Temporal Activity loses its Worker while a
`codex exec` turn is starting, executing a controlled command, or returning its
result? In particular, does a fresh launch or `codex exec resume <thread-id>`
prevent a second physical effect, or does the application need an authority
service that starts or attaches to one fenced executor?

## Invariant

For one stable logical session, turn, and effect:

- only the current monotonic generation and opaque capability may register a
  process or publish an effect, result, completion, or cancellation receipt;
- a redelivered Activity attaches to the current live execution rather than
  launching a competitor;
- replacement commits the new generation before the new process starts, so an
  old request remains stale even after an owner label recurs;
- cancellation revokes authority before signaling the exact process group and
  is acknowledged only after that group is verified empty; and
- the accepted Workflow outcome agrees with the independently read authority
  store, destination receipt, and workspace journal.

This is not an exactly-once claim. Temporal redelivery does not fence an
external process or make a destination effect idempotent. The protected arm's
application authority protocol supplies the tested safety property.

## Compared arms

`unsafe-fresh` starts a new `codex exec` process on every Activity delivery.
`explicit-thread-resume` durably records the thread learned from the first
delivery and passes it to `codex exec resume` on redelivery. It has no owner
fence or destination idempotency. `fenced-start-or-attach` moves process and
turn authority to a loopback supervisor outside the Worker. The supervisor
atomically allocates generation/capability leases, registers exact
PID/start/process-group identity, attaches replacement callers, fences before
replacement, and conditionally accepts effects and outcomes.

All three arms pin the same model, reasoning effort, structured-output schema,
CLI profile, controlled command, retry policy, and Temporal versions. The
authenticated arm records both the fixed wrapper and delegated CLI hashes. The
hermetic arm replaces only the vendor process with a deterministic JSONL
implementation so exact failure mechanics can be tested without model
nondeterminism.

Current-source HTTP fault boundaries additionally pre-register exact
point/session/generation/actor tuples and authenticate arrivals with one random
per-run credential. The credential is inherited through a read-once descriptor,
not Workflow input, CLI arguments, or portable evidence. Wrong credentials,
identity substitutions, arrival-ID reuse, and nonce replay fail before the
arrival count changes. The preserved v12 populations predate this hardening and
remain historical trusted-loopback evidence; a future current-source population
must use the authenticated boundary before making a new freshness claim.

## Exact failure boundaries

The controls run three unfaulted trials and three trials at each of:

1. `process-created-before-vendor-registration`, after the child exists and its
   process receipt is durable but before Codex emits `thread.started`;
2. `tool-effect-before-activity-completion`, after the controlled effect is
   independently visible but before the Activity returns; and
3. `final-output-before-activity-completion`, after a valid terminal Codex
   result is parsed but before Temporal records Activity completion.

The protected arm also repeats:

- `claim-committed-before-process-exec`;
- `codex-thread-observed-before-durable-registration`;
- `concurrent-recovery-at-effect-boundary`;
- `cancellation-while-executing`; and
- `authorized-process-failure-before-thread`.

Every injected failure waits on a named barrier. No sleep opens a failure
window. A stable inherited-file-descriptor gate prevents the launcher from
reaching the pre-thread barrier before its process identity is durably
registered.

## Oracle and evidence

The model's final message is not the oracle. Each sealed trial preserves and
cross-checks:

- logical session/turn/effect identity, generation, capability digest,
  Temporal attempt, Worker identity, PID/start identity, process group, and UTC
  event order;
- the exact Codex argument vector and raw JSONL, including one canonical thread
  ID and the expected controlled `command_execution` start/completion pair;
- barrier arrivals, process start/completion receipts, destination state,
  workspace journal, accepted outcome, and complete Workflow history; and
- SHA-256 identities for the harness, Worker, launcher, controlled effect,
  schema, wrapper, and delegated Codex CLI.

The independent disk auditor ignores the suite verdict, rehashes every raw
inventory, reconstructs the process/thread/effect counts, scans for capability
leaks, and replays every captured history. Missing or malformed evidence is
invalid, not a pass. The unsafe and resume-only controls distinguish the oracle
only when a faulted logical effect is physically applied twice while Temporal
accepts one outcome.

Raw roots contain nested Git repositories and ignored databases, so they are
never added directly to the outer repository. The evidence-transport command
creates deterministic archives, manifests, lineage, and audit bindings; verify
and restore must succeed before a package is admitted.

## Run

Build the pinned binaries:

```bash
make codex-direct
```

The experiment executable requires new evidence and audit paths plus explicit
Temporal, Worker, launcher, effect, Codex, profile, and schema paths. A typical
hermetic control invocation is:

```bash
bin/codex-direct-experiment \
  --evidence-root /tmp/codex-direct-hermetic-unsafe \
  --temporal-binary "$(command -v temporal)" \
  --worker-binary "$PWD/bin/codex-direct-worker" \
  --effect-binary "$PWD/bin/codex-direct-effect" \
  --launcher-binary "$PWD/bin/codex-direct-launcher" \
  --codex-binary "$PWD/bin/codex-direct-hermetic-codex" \
  --codex-wrapper "$PWD/bin/codex-direct-hermetic-codex" \
  --codex-home /tmp/new-empty-codex-home \
  --output-schema "$PWD/experiments/durable-vendor-sessions/codex-direct/schema/effect-result.schema.json" \
  --recovery-mode unsafe-fresh --trials 3 --hermetic
```

Use `explicit-thread-resume` or `fenced-start-or-attach` for the other arms.
Authenticated evidence additionally requires an already authenticated fixed
wrapper and delegated CLI/profile; the harness checks both login statuses and
hashes before starting.

Run the independent auditor outside the sealed root:

```bash
bin/codex-direct-evidence-audit \
  --evidence /tmp/codex-direct-hermetic-unsafe \
  --output /tmp/codex-direct-hermetic-unsafe-audit.json
```

The maintained correctness gate is:

```bash
make coverage-codex-direct
```

It runs race-instrumented unit and process/service tests, reconstructs admitted
transports, replays their histories, merges only complete coverage profiles,
and requires at least 80% statement coverage for both the lab and transport.

## Observed result

The final source-pinned hermetic population used one byte-reproducible v12
binary freeze. Unsafe fresh and explicit resume each produced 12 admitted runs:
six passes and six `duplicate_physical_effect` failures, 21 processes, 18
threads, 18 effects, and 12 replayed histories. Their independent audits
verified 405 and 429 raw artifacts respectively. Stable thread resume preserved
vendor transcript identity but did not supply turn authority or effect safety.

The protected hermetic arm passed all 27 runs. Its audit verified 846 raw
artifacts, 30 processes, 27 threads, 24 effects, 21 recovery attachments, three
fenced replacements, three cancellations, 27 replayed histories, and zero
capability leaks. A separate procfs check found no process whose command line
referenced the sealed root after suite teardown.

The serialized authenticated Codex CLI `0.147.0` population reproduced every
hermetic count and verdict under the fixed wrapper/profile and requested
`gpt-5.6-sol` model. Unsafe fresh and explicit resume each recorded six passes,
six duplicate-effect failures, 21 processes, 18 threads/effects, and 12 replayed
histories. The protected arm passed 27/27 with 30 processes, 27 threads, 24
effects, 21 attachments, three replacements, three cancellations, zero
capability leaks, and 27 replayed histories. Thus the final comparison contains
102 admitted runs and 102 replayed histories.

Six deterministic Git-safe v4 transports preserve the final populations and
their relevant rejected/superseded lineage. Independent rebuilds were
byte-identical; each package verified, restored, and reproduced its admitted
audit from restored bytes:

| Package | Files | Bytes | Run chains | Index SHA-256 |
| --- | ---: | ---: | ---: | --- |
| hermetic unsafe | 893 | 4,622,240 | 24 | `b5ef901e81762bc30160dc1ddbdd24bb007ea908c09a4b1578096673d12a80a1` |
| hermetic resume | 888 | 3,916,920 | 24 | `86379cd37aede290f2bc2ff9a0f655720e2b53265ce9a3e6a7df80ac19a4e13c` |
| hermetic fenced | 1,170 | 6,096,857 | 27 | `fc9f758d26c3cddbe4a2495cbf25f5f6cb25609d2cd9ace194f4c5456ae152a0` |
| authenticated unsafe | 965 | 5,476,328 | 24 | `0ea40ef0ab7521703dd8a21f20fb68714e86a2fe7f89a9bf3ad23fcdd73a6b9d` |
| authenticated resume | 888 | 3,936,586 | 24 | `16f6a2439eab98a621852bec04bfe1458626440e0cdc27e7ece6c9243a902d69` |
| authenticated fenced | 2,692 | 14,137,266 | 54 | `92b685a6ae934d880136669c5c113dbdab0066f5c1447be7c4d948e899bd6075` |

The 177 transport run chains include superseded complete populations; only the
102 final v12 runs support the current claim. Rejected roots remain preserved
but contribute no admitted run binding.

## Responsibility split

- Temporal durably records and replays Workflow procedure, detects Worker loss,
  and redelivers the Activity. It does not discover, attach to, or fence Codex.
- The application owns stable logical identity, generation/capability authority,
  start-or-attach, fence-before-replace, cancellation revocation, and
  conditional publication.
- The tested Bolt authority store and controlled destination/workspace protocols
  enforce the fence and stable effect ID at mutation time.
- Codex supplies the vendor thread and JSONL turn protocol. Thread resume alone
  is not treated as an owner token.

## Limits and falsifier

The runs use one contended Linux host, local Temporal dev servers, local Bolt
stores, and a loopback supervisor that survives Worker loss. They do not test
supervisor/host loss, cross-host routing, authority-store disaster recovery,
Codex App Server, deployment/version change, arbitrary external credentials,
or interactive PTY attachment. The workstation is not controlled compute, so
the study makes no relative latency, throughput, token-efficiency, or cost
claim.

The protected claim is falsified if a source-matched run launches two current
executors, accepts a stale/canceled effect or outcome, replaces before the new
fence commits, acknowledges cancellation while the original group remains
occupied or reuse is ambiguous, disagrees with independent destination or
workspace state, leaks a capability, or fails replay. The provider-compatibility
claim is also falsified if the recorded fixed-profile Codex version does not
reproduce the hermetic protected result in fresh serialized trials.
