package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrIncompatibleAgentBuild = errors.New("incompatible detached-agent build")
	registryBucket            = []byte("agent-session-v1")
	registryKey               = []byte("record")
)

type Registry struct {
	Path string
}

func (r Registry) StartOrAttach(ctx context.Context, request AttachRequest) (AttachReceipt, error) {
	if err := validateAttachRequest(request); err != nil {
		return AttachReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return AttachReceipt{}, err
	}
	database, err := bolt.Open(r.Path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return AttachReceipt{}, fmt.Errorf("open agent registry: %w", err)
	}
	defer database.Close()

	now := time.Now().UTC()
	receipt := AttachReceipt{SessionID: request.SessionID, WorkerBuild: request.WorkerBuild, ObservedAt: now}
	err = database.Update(func(transaction *bolt.Tx) error {
		bucket, createErr := transaction.CreateBucketIfNotExists(registryBucket)
		if createErr != nil {
			return createErr
		}
		encoded := bucket.Get(registryKey)
		if encoded == nil {
			record := AgentRecord{
				SessionID: request.SessionID, AgentBuild: request.AgentBuild,
				StartedByWorker: request.WorkerBuild, StartedAt: now,
			}
			receipt.AgentBuild, receipt.Action = record.AgentBuild, ActionStarted
			return putRecord(bucket, record)
		}
		var record AgentRecord
		if decodeErr := decodeRegistry(encoded, &record); decodeErr != nil {
			return fmt.Errorf("decode agent registry: %w", decodeErr)
		}
		if record.SessionID != request.SessionID {
			return fmt.Errorf("registry session %q does not match %q", record.SessionID, request.SessionID)
		}
		if !slices.Contains(request.CompatibleAgentBuilds, record.AgentBuild) {
			return fmt.Errorf("%w: worker %q does not accept stored build %q", ErrIncompatibleAgentBuild, request.WorkerBuild, record.AgentBuild)
		}
		record.Attachments = append(record.Attachments, Attachment{WorkerBuild: request.WorkerBuild, AttachedAt: now})
		receipt.AgentBuild, receipt.Action = record.AgentBuild, ActionAttached
		return putRecord(bucket, record)
	})
	if err != nil {
		return AttachReceipt{}, fmt.Errorf("start or attach: %w", err)
	}
	return receipt, nil
}

func (r Registry) Read() (AgentRecord, error) {
	database, err := bolt.Open(r.Path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return AgentRecord{}, fmt.Errorf("open agent registry: %w", err)
	}
	defer database.Close()
	var record AgentRecord
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket(registryBucket)
		if bucket == nil || bucket.Get(registryKey) == nil {
			return errors.New("agent registry is empty")
		}
		return decodeRegistry(bucket.Get(registryKey), &record)
	})
	return record, err
}

func decodeRegistry(encoded []byte, record *AgentRecord) error {
	if err := rejectDuplicateJSONKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(record); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func validateAttachRequest(request AttachRequest) error {
	if strings.TrimSpace(request.SessionID) == "" || strings.TrimSpace(request.AgentBuild) == "" || strings.TrimSpace(request.WorkerBuild) == "" {
		return errors.New("session, agent build, and worker build are required")
	}
	if !slices.Contains(request.CompatibleAgentBuilds, request.AgentBuild) {
		return fmt.Errorf("%w: worker %q cannot start build %q", ErrIncompatibleAgentBuild, request.WorkerBuild, request.AgentBuild)
	}
	return nil
}

func putRecord(bucket *bolt.Bucket, record AgentRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bucket.Put(registryKey, encoded)
}
