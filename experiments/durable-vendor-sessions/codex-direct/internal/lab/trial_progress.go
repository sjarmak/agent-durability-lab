package lab

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const trialProgressFile = "trial-progress.jsonl"

type trialProgress struct {
	RecordedAt time.Time `json:"recorded_at"`
	Stage      string    `json:"stage"`
	Detail     string    `json:"detail,omitempty"`
}

func appendTrialProgress(directory, stage, detail string) (returnErr error) {
	if directory == "" || stage == "" {
		return errors.New("trial progress directory and stage are required")
	}
	encoded, err := json.Marshal(trialProgress{
		RecordedAt: time.Now().UTC(), Stage: stage, Detail: detail,
	})
	if err != nil {
		return fmt.Errorf("encode trial progress: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(directory, trialProgressFile),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open trial progress: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("append trial progress: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync trial progress: %w", err)
	}
	return nil
}
