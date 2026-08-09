package lab

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var attemptsBucket = []byte("attempts")

func recordAttemptStart(path string, observation AttemptObservation) (returnErr error) {
	if observation.Attempt < 1 || observation.WorkerID == "" || observation.PID <= 0 || observation.StartedAt.IsZero() {
		return errors.New("attempt start requires attempt, Worker ID, PID, and timestamp")
	}
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open observation store: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	return database.Update(func(transaction *bolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists(attemptsBucket)
		if err != nil {
			return fmt.Errorf("create attempts bucket: %w", err)
		}
		key := attemptKey(observation.Attempt)
		if bucket.Get(key) != nil {
			return fmt.Errorf("attempt %d already recorded", observation.Attempt)
		}
		encoded, err := json.Marshal(observation)
		if err != nil {
			return fmt.Errorf("encode attempt start: %w", err)
		}
		if err := bucket.Put(key, encoded); err != nil {
			return fmt.Errorf("record attempt start: %w", err)
		}
		return nil
	})
}

func recordAttemptFinish(
	path string,
	attempt int32,
	requestedAt, respondedAt time.Time,
	result EffectResult,
	effectErr error,
) (returnErr error) {
	database, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open observation store: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	return database.Update(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(attemptsBucket)
		if bucket == nil {
			return errors.New("attempt store is missing")
		}
		key := attemptKey(attempt)
		existing := bucket.Get(key)
		if existing == nil {
			return fmt.Errorf("attempt %d start is missing", attempt)
		}
		var observation AttemptObservation
		if err := json.Unmarshal(existing, &observation); err != nil {
			return fmt.Errorf("decode attempt %d: %w", attempt, err)
		}
		if !observation.EffectRespondedAt.IsZero() {
			return fmt.Errorf("attempt %d finish already recorded", attempt)
		}
		observation.EffectRequestedAt = requestedAt
		observation.EffectRespondedAt = respondedAt
		observation.Receipt = result.Receipt
		observation.Outcome = result.Outcome
		if effectErr != nil {
			observation.Error = effectErr.Error()
		}
		encoded, err := json.Marshal(observation)
		if err != nil {
			return fmt.Errorf("encode attempt finish: %w", err)
		}
		if err := bucket.Put(key, encoded); err != nil {
			return fmt.Errorf("record attempt finish: %w", err)
		}
		return nil
	})
}

func readAttempts(path string) (attempts []AttemptObservation, returnErr error) {
	database, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open observation snapshot: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, database.Close())
	}()
	attempts = make([]AttemptObservation, 0, 2)
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(attemptsBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			var observation AttemptObservation
			if err := json.Unmarshal(value, &observation); err != nil {
				return fmt.Errorf("decode attempt observation: %w", err)
			}
			attempts = append(attempts, observation)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func attemptKey(attempt int32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(attempt))
	return key
}
