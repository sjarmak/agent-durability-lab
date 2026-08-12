package main

import "strconv"

type Destination string

type Recipe struct {
	Destination    Destination
	EvidencePrefix string
	RetryOutcome   string
	Mechanism      string
	Atomicity      string
	Lookup         string
	Serialization  string
	Retention      string
	Conflict       string
	Artifacts      string
	Limits         string
}

func recipes() []Recipe {
	return []Recipe{
		{
			Destination: "idempotent-api", EvidencePrefix: "external-effects-20260806-v1", RetryOutcome: "deduplicated",
			Mechanism:     "Send the stable effect ID as the API idempotency key.",
			Atomicity:     "The destination atomically stores the key, mutation, and response receipt.",
			Lookup:        "The retry repeats the request; the destination looks up the key.",
			Serialization: "The destination serializes competing requests for one key.",
			Retention:     "Keep the idempotency record for the full retry/redelivery horizon.",
			Conflict:      "Reject reuse of the key when the effect ID or payload differs.",
			Artifacts:     "Preserve both attempt receipts and the exported destination-state.json.",
			Limits:        "Safety ends when key retention expires or the real API does not bind key, mutation, and receipt atomically.",
		},
		{
			Destination: "non-idempotent-api", EvidencePrefix: "external-effects-20260806-v1", RetryOutcome: "reconciled",
			Mechanism:     "Before retrying POST, query a stable correlation ID and reuse the recorded receipt.",
			Atomicity:     "The destination durably records correlation ID, payload identity, and receipt with the append.",
			Lookup:        "The destination exposes a strongly consistent correlation-ID lookup.",
			Serialization: "Only sequential retry is covered; concurrent same-ID callers must be excluded externally.",
			Retention:     "Retain correlation records through every possible retry and reconciliation.",
			Conflict:      "Reject a lookup whose stored payload hash differs from the retry payload.",
			Artifacts:     "Preserve both attempt receipts and the exported append log in destination-state.json.",
			Limits:        "Check-then-POST is unsafe for concurrent callers and impossible without stable lookup.",
		},
		{
			Destination: "database", EvidencePrefix: "external-effects-20260806-v1", RetryOutcome: "deduplicated",
			Mechanism:     "Use the stable effect ID as a unique row key in the mutation transaction.",
			Atomicity:     "Uniqueness check, mutation, and receipt commit in one database transaction.",
			Lookup:        "A retry reads the row keyed by the effect ID inside the transaction.",
			Serialization: "The database serializes conflicting writes through its transaction and uniqueness semantics.",
			Retention:     "Retain the effect row while any retry or delayed delivery can arrive.",
			Conflict:      "Reject an existing effect ID with different payload content.",
			Artifacts:     "Preserve both attempt receipts and the portable destination-state.json snapshot, not the runtime database.",
			Limits:        "No atomicity is established across this transaction and a separate remote destination.",
		},
		{
			Destination: "git", EvidencePrefix: "external-effects-20260806-v2", RetryOutcome: "reconciled",
			Mechanism:     "Write a stable marker path, validate its content, and reuse the commit receipt.",
			Atomicity:     "The tested boundary is after the marker commit; file-write-before-commit failure is outside it.",
			Lookup:        "A retry reads the marker and resolves the commit that contains it.",
			Serialization: "Access to the worktree must be serialized; concurrent writers are not covered.",
			Retention:     "Keep the marker and reachable commit for the retry/reconciliation horizon.",
			Conflict:      "Reject a stable marker whose content differs from the retry payload.",
			Artifacts:     "Preserve both attempt receipts, destination-state.json, and a verified Git bundle.",
			Limits:        "This does not cover concurrent worktree writers or failure between file write and commit.",
		},
		{
			Destination: "message", EvidencePrefix: "external-effects-20260806-v2", RetryOutcome: "deduplicated",
			Mechanism:     "Publish with the stable effect ID as the destination message ID.",
			Atomicity:     "The destination atomically retains message ID, publication, and receipt.",
			Lookup:        "The publisher retries the same message ID; destination deduplication returns the receipt.",
			Serialization: "The destination serializes competing publications for one message ID.",
			Retention:     "Deduplication retention must exceed the complete redelivery horizon.",
			Conflict:      "Reject reuse of a message ID with different content.",
			Artifacts:     "Preserve both attempt receipts and the exported publication log in destination-state.json.",
			Limits:        "The hermetic publisher says nothing about real broker retention, consumer acknowledgements, or processing semantics.",
		},
		{
			Destination: "artifact", EvidencePrefix: "external-effects-20260806-v2", RetryOutcome: "deduplicated",
			Mechanism:     "Publish a content-addressed blob and bind the stable effect ID to it with a reference.",
			Atomicity:     "Each file is published exclusively and durably; blob plus reference are not one atomic operation.",
			Lookup:        "A retry resolves the stable reference, then validates the referenced blob content.",
			Serialization: "Exclusive publication rejects competing inconsistent content for one path.",
			Retention:     "Retain both the reference and blob through retries; orphan collection is a separate protocol.",
			Conflict:      "Reject a reference or blob whose stored content differs from the requested content.",
			Artifacts:     "Preserve both attempt receipts plus the actual blobs, references, and destination-state.json.",
			Limits:        "The tested fault is after blob and reference publication, not the partial-publication window or a remote object store.",
		},
	}
}

func recipeFor(destination Destination) (Recipe, bool) {
	for _, recipe := range recipes() {
		if recipe.Destination == destination {
			return recipe, true
		}
	}
	return Recipe{}, false
}

func evidenceRunName(recipe Recipe, mode string, trial int) string {
	return recipe.EvidencePrefix + "-" + string(recipe.Destination) + "-" + mode + "-trial-" + strconv.Itoa(trial)
}
