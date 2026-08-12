package lab

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type ThreadReceipt struct {
	ThreadID          string    `json:"thread_id"`
	PhysicalAttemptID string    `json:"physical_attempt_id"`
	ActorID           string    `json:"actor_id"`
	PID               int       `json:"pid"`
	ProcessStart      string    `json:"process_start"`
	ProcessIdentity   string    `json:"process_identity"`
	ObservedAt        time.Time `json:"observed_at"`
}

func ReadThreadReceipt(path string) (ThreadReceipt, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ThreadReceipt{}, fmt.Errorf("read Codex thread receipt: %w", err)
	}
	if len(data) == 0 || len(data) > 64<<10 {
		return ThreadReceipt{}, errors.New("codex thread receipt is not bounded")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var receipt ThreadReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return ThreadReceipt{}, fmt.Errorf("decode Codex thread receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ThreadReceipt{}, errors.New("codex thread receipt contains trailing data")
	}
	if !receipt.valid() {
		return ThreadReceipt{}, errors.New("codex thread receipt is incomplete")
	}
	return receipt, nil
}

func (r ThreadReceipt) valid() bool {
	return validThreadID(r.ThreadID) && r.PhysicalAttemptID != "" && r.ActorID != "" && r.PID > 0 && r.ProcessStart != "" &&
		r.ProcessIdentity != "" && !r.ObservedAt.IsZero()
}
