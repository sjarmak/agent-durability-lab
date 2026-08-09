package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrPhysicalAttemptExists = errors.New("physical effect attempt already exists")

var attemptsBucket = []byte("attempts")

type EffectAttempt struct {
	LogicalSessionID  string    `json:"logical_session_id"`
	LogicalTurnID     string    `json:"logical_turn_id"`
	LogicalEffectID   string    `json:"logical_effect_id"`
	PhysicalAttemptID string    `json:"physical_attempt_id"`
	ActorID           string    `json:"actor_id"`
	ProcessIdentity   string    `json:"process_identity"`
	Applied           bool      `json:"applied"`
	AppliedAt         time.Time `json:"applied_at"`
}

type DestinationSnapshot struct {
	Attempts []EffectAttempt `json:"attempts"`
}

func CommitEffect(ctx context.Context, path string, attempt EffectAttempt) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(path) == "" || !attempt.valid() {
		return errors.New("destination path and complete effect attempt are required")
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open controlled destination: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()

	applied := attempt
	applied.Applied = true
	encoded, err := json.Marshal(applied)
	if err != nil {
		return fmt.Errorf("encode effect attempt: %w", err)
	}
	err = database.Update(func(transaction *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket, err := transaction.CreateBucketIfNotExists(attemptsBucket)
		if err != nil {
			return err
		}
		key := []byte(applied.PhysicalAttemptID)
		if bucket.Get(key) != nil {
			return ErrPhysicalAttemptExists
		}
		return bucket.Put(key, encoded)
	})
	if err != nil {
		return fmt.Errorf("commit controlled effect: %w", err)
	}
	return nil
}

func ReadDestination(ctx context.Context, path string) (snapshot DestinationSnapshot, returnErr error) {
	if err := ctx.Err(); err != nil {
		return DestinationSnapshot{}, err
	}
	if strings.TrimSpace(path) == "" {
		return DestinationSnapshot{}, errors.New("destination path is required")
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return DestinationSnapshot{}, fmt.Errorf("open controlled destination: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()

	attempts := make([]EffectAttempt, 0)
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(attemptsBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			var attempt EffectAttempt
			if err := json.Unmarshal(value, &attempt); err != nil {
				return fmt.Errorf("decode effect attempt: %w", err)
			}
			attempts = append(attempts, attempt)
			return nil
		})
	})
	if err != nil {
		return DestinationSnapshot{}, err
	}
	sort.Slice(attempts, func(left, right int) bool {
		if attempts[left].AppliedAt.Equal(attempts[right].AppliedAt) {
			return attempts[left].PhysicalAttemptID < attempts[right].PhysicalAttemptID
		}
		return attempts[left].AppliedAt.Before(attempts[right].AppliedAt)
	})
	return DestinationSnapshot{Attempts: attempts}, nil
}

func (a EffectAttempt) valid() bool {
	return a.LogicalSessionID != "" && a.LogicalTurnID != "" && a.LogicalEffectID != "" &&
		a.PhysicalAttemptID != "" && a.ActorID != "" && a.ProcessIdentity != "" &&
		!a.AppliedAt.IsZero()
}
