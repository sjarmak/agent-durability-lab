package lab

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrStaleAttempt  = errors.New("stale activity attempt")
	ErrOwnerConflict = errors.New("different owner already registered for activity attempt")
)

var (
	attemptBucket  = []byte("attempt_fence")
	activeKey      = []byte("active_attempt")
	activeOwnerKey = []byte("active_owner_hash")
)

type AttemptFence struct {
	db *bolt.DB
}

func OpenAttemptFence(path string) (*AttemptFence, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open attempt fence: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(attemptBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize attempt fence: %w", err)
	}
	return &AttemptFence{db: db}, nil
}

func (f *AttemptFence) Register(ctx context.Context, attempt int32, ownerToken string) error {
	if attempt <= 0 {
		return errors.New("attempt must be positive")
	}
	if ownerToken == "" {
		return errors.New("owner token must not be empty")
	}
	ownerHash := hashOwnerToken(ownerToken)
	return f.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket := tx.Bucket(attemptBucket)
		active := decodeAttempt(bucket.Get(activeKey))
		if attempt < active {
			return ErrStaleAttempt
		}
		if attempt == active {
			if subtle.ConstantTimeCompare(bucket.Get(activeOwnerKey), []byte(ownerHash)) != 1 {
				return ErrOwnerConflict
			}
			return nil
		}
		var encoded [4]byte
		binary.BigEndian.PutUint32(encoded[:], uint32(attempt))
		if err := bucket.Put(activeKey, encoded[:]); err != nil {
			return err
		}
		return bucket.Put(activeOwnerKey, []byte(ownerHash))
	})
}

func (f *AttemptFence) Authorize(ctx context.Context, ownerToken string) error {
	if ownerToken == "" {
		return ErrStaleAttempt
	}
	ownerHash := hashOwnerToken(ownerToken)
	return f.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket := tx.Bucket(attemptBucket)
		if decodeAttempt(bucket.Get(activeKey)) == 0 ||
			subtle.ConstantTimeCompare(bucket.Get(activeOwnerKey), []byte(ownerHash)) != 1 {
			return ErrStaleAttempt
		}
		return nil
	})
}

func (f *AttemptFence) Close() error {
	return f.db.Close()
}

func decodeAttempt(value []byte) int32 {
	if len(value) != 4 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value))
}

func hashOwnerToken(ownerToken string) string {
	hash := sha256.Sum256([]byte(ownerToken))
	return hex.EncodeToString(hash[:])
}
