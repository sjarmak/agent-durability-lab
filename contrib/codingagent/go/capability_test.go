package codingagent

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCapabilityIsOpaqueAndVerifiable(t *testing.T) {
	capability, err := NewCapability()
	if err != nil {
		t.Fatal(err)
	}
	secret := capability.ExportSecret()
	if secret == "" || strings.Contains(capability.String(), secret) {
		t.Fatal("capability must be exportable explicitly but redacted by String")
	}
	if !capability.Matches(capability.Digest()) {
		t.Fatal("capability did not match its digest")
	}
	other, err := NewCapability()
	if err != nil {
		t.Fatal(err)
	}
	if capability.Matches(other.Digest()) {
		t.Fatal("capability matched another capability digest")
	}
	restored, err := ParseCapability(secret)
	if err != nil {
		t.Fatal(err)
	}
	if !restored.Matches(capability.Digest()) {
		t.Fatal("restored capability did not match")
	}
	if _, err := json.Marshal(capability); err == nil {
		t.Fatal("raw capability serialized to JSON")
	}
	if got := fmt.Sprintf("%#v", capability); got != "codingagent.Capability([REDACTED])" || strings.Contains(got, secret) {
		t.Fatalf("Go-syntax formatting exposed capability: %s", got)
	}
	if reflect.TypeOf(capability).Comparable() {
		t.Fatal("capability permits non-constant-time == comparison")
	}
}

func TestParseCapabilityRejectsWeakInput(t *testing.T) {
	for _, input := range []string{"", "guessable", strings.Repeat("a", 128)} {
		if _, err := ParseCapability(input); err == nil {
			t.Fatalf("ParseCapability(%q) succeeded", input)
		}
	}
}
