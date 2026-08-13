package lab

import (
	"errors"
	"testing"
)

func TestSourcePinsAreExactAndMutationSensitive(t *testing.T) {
	t.Parallel()

	pins, err := CaptureSourcePins()
	if err != nil {
		t.Fatalf("CaptureSourcePins: %v", err)
	}
	if len(pins) < 20 || pins["go.mod"] == "" || pins["experiments/large-artifact-durability/internal/lab/source_pins.go"] == "" {
		t.Fatalf("source pins are incomplete: %v", pins)
	}
	if err := ValidateCurrentSourcePins(pins); err != nil {
		t.Fatalf("ValidateCurrentSourcePins: %v", err)
	}
	pins["go.mod"] = "invalid"
	if err := ValidateCurrentSourcePins(pins); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("mutated source error = %v, want ErrArtifactConflict", err)
	}
}
