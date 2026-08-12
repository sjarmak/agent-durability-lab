package codingagent_test

import (
	"context"
	"testing"
	"time"

	codingagent "github.com/sjarmak/temporal_projects/contrib/codingagent/go"
)

func TestPublicClaimStartPath(t *testing.T) {
	capability, err := codingagent.NewCapability()
	if err != nil {
		t.Fatal(err)
	}
	claim := codingagent.Command{
		Operation:       codingagent.OperationClaim,
		Identity:        codingagent.Identity{SessionID: "session:external", TurnID: "turn:external", OperationID: "operation:claim"},
		RequestHash:     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		ActorGeneration: 1, ReceiptID: "receipt:claim", OccurredAt: time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC),
	}
	kernel, outcome, err := codingagent.NewKernel().Apply(context.Background(), claim.AsCoordinator().WithNewCapability(capability))
	if err != nil || outcome.Disposition != codingagent.DispositionAccepted {
		t.Fatalf("claim: %#v, %v", outcome, err)
	}
	start := codingagent.Command{
		Operation:       codingagent.OperationBeginStart,
		Identity:        codingagent.Identity{SessionID: "session:external", TurnID: "turn:external", OperationID: "operation:start"},
		RequestHash:     "sha256:3333333333333333333333333333333333333333333333333333333333333333",
		ActorGeneration: 1, Capability: capability, ReceiptID: "receipt:start",
		OccurredAt: time.Date(2026, 8, 11, 12, 0, 1, 0, time.UTC),
	}
	kernel, outcome, err = kernel.Apply(context.Background(), start)
	if err != nil || outcome.Disposition != codingagent.DispositionAccepted {
		t.Fatalf("begin start: %#v, %v", outcome, err)
	}
	state, ok := kernel.State()
	if !ok || state.Lifecycle != codingagent.LifecycleStarting || state.OwnerCapabilityDigest != capability.Digest() {
		t.Fatalf("public state: %#v, %v", state, ok)
	}
}
