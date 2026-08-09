package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
)

var databaseEffectsBucket = []byte("effects")

func applyDatabaseEffect(path string, request EffectRequest) (result EffectResult, returnErr error) {
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return EffectResult{}, fmt.Errorf("open effect database: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	err = database.Update(func(transaction *bolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists(databaseEffectsBucket)
		if err != nil {
			return fmt.Errorf("create effects bucket: %w", err)
		}
		key := request.EffectID
		if request.Mode == ModeUnsafe {
			key += "/attempt-" + strconv.Itoa(int(request.Attempt))
		} else if existing := bucket.Get([]byte(key)); existing != nil {
			var effect PhysicalEffect
			if err := json.Unmarshal(existing, &effect); err != nil {
				return fmt.Errorf("decode existing database effect: %w", err)
			}
			if effect.LogicalID != request.EffectID || effect.Payload != request.Payload {
				return fmt.Errorf("database effect ID %q has conflicting content", request.EffectID)
			}
			result = EffectResult{Receipt: effect.Receipt, Outcome: OutcomeDeduplicated}
			return nil
		}
		effect := PhysicalEffect{
			PhysicalID: key, LogicalID: request.EffectID, Receipt: "database:" + key,
			Payload: request.Payload, AppliedAt: time.Now().UTC(), Attempt: request.Attempt,
			Kind: DestinationDatabase,
		}
		encoded, err := json.Marshal(effect)
		if err != nil {
			return fmt.Errorf("encode database effect: %w", err)
		}
		if err := bucket.Put([]byte(key), encoded); err != nil {
			return fmt.Errorf("store database effect: %w", err)
		}
		result = EffectResult{Receipt: effect.Receipt, Outcome: OutcomeApplied}
		return nil
	})
	if err != nil {
		return EffectResult{}, err
	}
	return result, nil
}

func snapshotDatabaseDestination(path string) (state DestinationState, returnErr error) {
	database, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return DestinationState{}, fmt.Errorf("open effect database snapshot: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	state = DestinationState{PhysicalEffects: make([]PhysicalEffect, 0)}
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(databaseEffectsBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			var effect PhysicalEffect
			if err := json.Unmarshal(value, &effect); err != nil {
				return fmt.Errorf("decode database effect: %w", err)
			}
			state.PhysicalEffects = append(state.PhysicalEffects, effect)
			return nil
		})
	})
	if err != nil {
		return DestinationState{}, err
	}
	return state, nil
}
