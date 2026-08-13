package lab

import (
	"context"
	"errors"
)

const MaxArtifactBytes = 32 << 20

var (
	ErrInvalidArtifact  = errors.New("invalid artifact request")
	ErrArtifactConflict = errors.New("artifact identity conflicts with durable state")
)

type Mode string

const (
	ModeUnsafe    Mode = "unsafe"
	ModeProtected Mode = "protected"
)

func (m Mode) valid() bool {
	return m == ModeUnsafe || m == ModeProtected
}

func (m Mode) Valid() bool {
	return m.valid()
}

type Boundary string

const (
	BoundaryBlobPublished            Boundary = "blob_published"
	BoundaryReferenceCreated         Boundary = "reference_created"
	BoundaryReferencePublished       Boundary = "reference_published"
	BoundaryActivityCompleted        Boundary = "activity_completed"
	BoundaryAcknowledgementPublished Boundary = "acknowledgement_published"
	BoundaryExternalStorageStored    Boundary = "external_storage_stored"
)

type BoundaryHook func(context.Context, Boundary, StoreSnapshot) error

type ProduceRequest struct {
	LogicalID string
	Content   []byte
	Attempt   int32
	Mode      Mode
}

type ArtifactReference struct {
	LogicalID     string `json:"logical_id"`
	Digest        string `json:"digest"`
	BlobName      string `json:"blob_name"`
	ReferenceName string `json:"reference_name"`
	Size          int64  `json:"size"`
}

type AcknowledgeRequest struct {
	Reference  ArtifactReference
	ConsumerID string
	Attempt    int32
	Mode       Mode
}

type Acknowledgement struct {
	LogicalID     string `json:"logical_id"`
	Digest        string `json:"digest"`
	ConsumerID    string `json:"consumer_id"`
	ReferenceName string `json:"reference_name"`
}

type StoredEntry struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type StoreSnapshot struct {
	Blobs             []StoredEntry `json:"blobs"`
	PendingReferences []StoredEntry `json:"pending_references"`
	References        []StoredEntry `json:"references"`
	Acknowledgements  []StoredEntry `json:"acknowledgements"`
}

type ReconcileReport struct {
	ReachableBlobs           int      `json:"reachable_blobs"`
	RemovedBlobs             []string `json:"removed_blobs"`
	RemovedPendingReferences []string `json:"removed_pending_references"`
}

type WorkflowInput struct {
	StoreRoot       string   `json:"store_root"`
	SourcePath      string   `json:"source_path"`
	LogicalID       string   `json:"logical_id"`
	ConsumerID      string   `json:"consumer_id"`
	Mode            Mode     `json:"mode"`
	FailureBoundary Boundary `json:"failure_boundary"`
}

type ConsumeInput struct {
	StoreRoot       string            `json:"store_root"`
	Reference       ArtifactReference `json:"reference"`
	ConsumerID      string            `json:"consumer_id"`
	Mode            Mode              `json:"mode"`
	FailureBoundary Boundary          `json:"failure_boundary"`
}

type WorkflowResult struct {
	Reference       ArtifactReference `json:"reference"`
	Acknowledgement Acknowledgement   `json:"acknowledgement"`
}

type ExternalWorkflowInput struct {
	SourcePath string `json:"source_path"`
}

type ExternalWorkflowResult struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}
