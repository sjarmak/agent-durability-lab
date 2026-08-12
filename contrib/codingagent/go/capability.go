package codingagent

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const capabilityBytes = 32

// Capability is a 256-bit bearer secret. Only its digest belongs in Temporal
// history, protocol records, logs, or evidence.
type Capability struct {
	secret [capabilityBytes]byte
	valid  bool
	_      [0]func() // Keep bearer values non-comparable by construction.
}

func NewCapability() (Capability, error) {
	var capability Capability
	if _, err := rand.Read(capability.secret[:]); err != nil {
		return Capability{}, fmt.Errorf("generate owner capability: %w", err)
	}
	capability.valid = true
	return capability, nil
}

func ParseCapability(encoded string) (Capability, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != capabilityBytes {
		return Capability{}, errors.New("owner capability must be a base64url-encoded 256-bit secret")
	}
	var capability Capability
	copy(capability.secret[:], decoded)
	capability.valid = true
	return capability, nil
}

// ExportSecret exposes the bearer value for storage in an application-owned
// secret boundary. It must never be put in Workflow arguments or results.
func (c Capability) ExportSecret() string {
	if !c.valid {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(c.secret[:])
}

func (c Capability) String() string { return "[REDACTED owner capability]" }

func (c Capability) GoString() string { return "codingagent.Capability([REDACTED])" }

func (c Capability) Digest() Digest {
	if !c.valid {
		return ""
	}
	sum := sha256.Sum256(c.secret[:])
	return Digest("sha256:" + hex.EncodeToString(sum[:]))
}

func (c Capability) Matches(digest Digest) bool {
	if !c.valid {
		return false
	}
	want, err := decodeDigest(digest)
	if err != nil {
		return false
	}
	got := sha256.Sum256(c.secret[:])
	return subtle.ConstantTimeCompare(got[:], want) == 1
}

func (Capability) MarshalJSON() ([]byte, error) {
	return nil, errors.New("owner capabilities cannot be serialized to JSON")
}

func (c *Capability) UnmarshalJSON([]byte) error {
	*c = Capability{}
	return errors.New("owner capabilities cannot be deserialized from JSON")
}

var _ json.Marshaler = Capability{}
