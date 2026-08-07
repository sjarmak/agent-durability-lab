package lab

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

type capturedAttempt struct {
	observation AttemptObservation
	taskToken   []byte
	ownerToken  string
}

type attemptRecorder struct {
	fence    *AttemptFence
	attempts chan<- capturedAttempt
}

func (r attemptRecorder) Run(ctx context.Context) (string, error) {
	info := activity.GetInfo(ctx)
	ownerToken, err := newOwnerToken()
	if err != nil {
		return "", err
	}
	if err := r.fence.Register(ctx, info.Attempt, ownerToken); err != nil {
		return "", fmt.Errorf("register attempt %d: %w", info.Attempt, err)
	}
	token := append([]byte(nil), info.TaskToken...)
	hash := sha256.Sum256(token)
	attempt := capturedAttempt{
		observation: AttemptObservation{
			Attempt:        info.Attempt,
			ObservedAt:     time.Now().UTC(),
			TaskTokenHash:  hex.EncodeToString(hash[:]),
			OwnerTokenHash: hashOwnerToken(ownerToken),
		},
		taskToken:  token,
		ownerToken: ownerToken,
	}
	select {
	case r.attempts <- attempt:
		return "", activity.ErrResultPending
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func newOwnerToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate owner token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}
