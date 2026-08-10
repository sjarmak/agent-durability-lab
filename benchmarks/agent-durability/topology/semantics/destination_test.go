package semantics

import (
	"testing"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/topology/protocol"
)

func TestDestinationFencesExecutingSupersessionAndKeepsReplacementLive(t *testing.T) {
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		t.Run(string(probe), func(t *testing.T) {
			destination := NewMemoryDestination()
			oldAuthority := Authority{Generation: 1, CapabilityHash: repeatedHash('a')}
			newAuthority := Authority{Generation: 2, CapabilityHash: repeatedHash('b')}
			if err := destination.SetAuthority("item-001", oldAuthority); err != nil {
				t.Fatal(err)
			}
			if err := destination.Supersede("item-001", oldAuthority, newAuthority); err != nil {
				t.Fatal(err)
			}
			stale, err := destination.ApplyEffect(EffectRequest{
				EventID: "stale-event", ItemID: "item-001", LogicalEffectID: "effect-old", Authority: oldAuthority, Probe: probe,
			})
			if err != nil {
				t.Fatal(err)
			}
			replacement, err := destination.ApplyEffect(EffectRequest{
				EventID: "replacement-event", ItemID: "item-001", LogicalEffectID: "effect-new", Authority: newAuthority, Probe: probe,
			})
			if err != nil {
				t.Fatal(err)
			}
			if probe == protocol.ProbeProtected && (stale.Decision != protocol.DecisionRejected || stale.Applied) {
				t.Fatalf("protected stale action = %+v", stale)
			}
			if probe == protocol.ProbeUnsafe && (stale.Decision != protocol.DecisionAccepted || !stale.Applied) {
				t.Fatalf("unsafe stale action = %+v", stale)
			}
			if replacement.Decision != protocol.DecisionAccepted || !replacement.Applied {
				t.Fatalf("replacement action = %+v", replacement)
			}
		})
	}
}

func TestDestinationSupersessionIsIdempotentForActivityRetry(t *testing.T) {
	destination := NewMemoryDestination()
	oldAuthority := Authority{Generation: 1, CapabilityHash: repeatedHash('a')}
	newAuthority := Authority{Generation: 2, CapabilityHash: repeatedHash('b')}
	if err := destination.SetAuthority("item-001", oldAuthority); err != nil {
		t.Fatal(err)
	}
	if err := destination.Supersede("item-001", oldAuthority, newAuthority); err != nil {
		t.Fatal(err)
	}
	if err := destination.Supersede("item-001", oldAuthority, newAuthority); err != nil {
		t.Fatalf("retry identical supersession: %v", err)
	}
}

func TestDestructiveTransitionReconcilesStableReceiptOrRepeatsUnsafeApply(t *testing.T) {
	for _, probe := range []protocol.Probe{protocol.ProbeUnsafe, protocol.ProbeProtected} {
		t.Run(string(probe), func(t *testing.T) {
			destination := NewMemoryDestination()
			authority := Authority{Generation: 1, CapabilityHash: repeatedHash('a')}
			if err := destination.SetAuthority("item-001", authority); err != nil {
				t.Fatal(err)
			}
			request := DestructiveRequest{
				EventID: "destructive-attempt-1", ItemID: "item-001", OperationID: "delete/item-001", Authority: authority,
				ExpectedVersion: 0, Attempt: 1, Probe: probe,
			}
			first, err := destination.ApplyDestructive(request)
			if err != nil {
				t.Fatal(err)
			}
			request.EventID, request.Attempt = "destructive-attempt-2", 2
			second, err := destination.ApplyDestructive(request)
			if err != nil {
				t.Fatal(err)
			}
			state := destination.Snapshot()
			if probe == protocol.ProbeProtected {
				if !first.Applied || second.Applied || second.Decision != protocol.DecisionReconciled ||
					first.ReceiptID != second.ReceiptID || state.Version != 1 || state.DestructiveApplyCount != 1 {
					t.Fatalf("protected destructive state: first=%+v second=%+v state=%+v", first, second, state)
				}
			} else if !first.Applied || !second.Applied || first.ReceiptID == second.ReceiptID ||
				state.Version != 2 || state.DestructiveApplyCount != 2 {
				t.Fatalf("unsafe destructive state: first=%+v second=%+v state=%+v", first, second, state)
			}
		})
	}
}

func repeatedHash(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return string(value)
}
