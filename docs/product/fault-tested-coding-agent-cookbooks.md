# Fault-Tested Durability Patterns for Coding Agents

**Status:** candidate product surface; final verification tracked by
`temporal_projects-bln.6`

## Position

This repository is the fault-tested, evidence-backed reference for building
coding agents on Temporal. It is deliberately narrower than a general agentic
cookbook: it answers whether the whole application remains correct when a
Worker, process, acknowledgement, or destination boundary fails.

The primary user is a backend or platform engineer accountable for a long-running
coding agent. Their first product journey is first trustworthy recovery: from a
fresh checkout, run one declared fault, observe the unsafe
design fail, observe the protected design pass, replay the native history, and
see which guarantee belongs to Temporal, the application, or the destination.

## Why this product now

The lab already has a portable protocol, Go and Python bindings, six executable
cookbooks, exact fault barriers, negative controls, append-only evidence, and
admitted findings. The missing layer is presentation: a fast path that makes the
evidence legible without turning a polished example into a stronger claim.

Developer-oriented agent cookbooks demonstrate useful presentation patterns:
short entry points, progressive tutorials, reusable scenarios, and a browsable
result surface. This product adopts those interaction patterns, but keeps its
own research contract. No third-party code is imported without a compatible
license and an explicit dependency decision.

## Information architecture

| Section | User question | Primary surface |
| --- | --- | --- |
| Start | How do I see one trustworthy recovery? | Credential-free quickstart and Dev Container |
| Patterns | What must my application own? | Six durability cookbooks |
| Scenarios | Which fault am I testing? | Shared unfaulted, unsafe, and protected cases |
| Evidence | Why should I believe the verdict? | Normalized trace, native history, raw artifacts, and lineage |
| Protocol | What is portable across SDKs? | Versioned schema, fixtures, and bindings |
| Research | What is established, bounded, or still open? | Findings, guarantee ledger, and experiments |

## Presentation boundary

```text
sealed raw evidence
        |
        v
independent verifier/oracle
        |
        v
immutable presentation catalog
        |
        v
read-only explorer and tutorials
```

The explorer is **not an oracle**. It renders only verified evidence and cannot
promote a claim, rewrite a verdict, omit the unsafe negative control, replace
native history with a normalized trace, or hide correction lineage. Each view
retains the invariant, exact barrier, responsibility split, and falsifier.

## First product slice

The first slice contains:

1. a credential-free scenario with matching unfaulted, unsafe, and protected
   episodes;
2. one command and a Dev Container/Codespaces path that run the same checks;
3. a shared scenario contract for identity, authority, effects, cancellation,
   terminal outcome, artifacts, and replay;
4. a read-only evidence explorer showing the normalized event sequence beside
   the native history and provenance; and
5. tutorials and Code Exchange packaging that link back to the exact admitted
   evidence rather than presenting screenshots as proof.

The implemented first journey is
[`cookbooks/coding-agents/quickstart.sh`](../../cookbooks/coding-agents/quickstart.sh).
It audits six final v12 Codex transports, requires exact receipts for every
subtest, replays 102 histories, and only then renders the selected recovery
triad. It is a walkthrough over admitted evidence, not a new experiment.

The pinned [development workspace](../../.devcontainer/README.md) runs that same
credential-free path in Codespaces, VS Code Dev Containers, or local Docker,
then applies the focused presentation and contract gates. Container resources
improve onboarding consistency; they are not controlled-compute evidence.

The implemented [recovery evidence explorer](../../cookbooks/coding-agents/explorer/README.md)
adds episode and evidence-view selection, timeline navigation, unsafe-versus-protected
comparison, direct native-history/raw-record access, provenance, responsibility, and the
falsifier. It is a loopback-only read-only adapter over the same catalog and sealed transports.

The [failure-first tutorials](../../cookbooks/coding-agents/tutorials/README.md) connect that
walkthrough to the six implementation patterns. The local
[Code Exchange preview](code-exchange-submission.md) mirrors the current community submission
contract, uses the repository's MIT license and public canonical link, and is not an external
submission.

## Acceptance

The slice is successful when a fresh-checkout user can:

- run the required scenario without provider credentials;
- see the unsafe valid-fail and protected valid-pass under the same fault;
- inspect stable logical identity separately from Activity attempt, Worker,
  process, and provider identity;
- identify the authority transition and destination effect receipt;
- replay the captured native history;
- state the responsibility of Temporal, application code, and the destination;
  and
- name the observable falsifier and the evidence correction lineage.

Required checks fail closed on skipped scenarios, unverified evidence, malformed
or unconfined artifact references, replay failure, and missing provenance.

## Explicit anti-goals

This work does not pursue generic runtime parity, a generic agent framework,
exactly once effects, or controlled-compute performance claims. Broad provider compatibility
is also out of scope, as are credential durability, cross-host supervisor
recovery, and a hosted agent control plane. A read-only evidence explorer is in
scope; a mutable operational UI is not.

## Product risks

- **Normalization drift:** the presentation contract is downstream of the
  independent verifier and retains links to raw artifacts and native history.
- **Evidence-path attacks:** decoders are bounded, strict, and treat artifact
  paths as metadata until confinement has been verified.
- **Evidence overload:** the default journey shows one exact fault and one
  responsibility boundary, with raw detail available progressively.
- **Resource assumptions:** correctness runs remain separate from controlled
  compute and timing research.
