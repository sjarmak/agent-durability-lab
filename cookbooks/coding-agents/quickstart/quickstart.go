package quickstart

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/sjarmak/temporal_projects/cookbooks/coding-agents/presentation"
)

const (
	maxReceiptLineBytes = 1 << 20
	maxReceiptBytes     = 32 << 20
	ReplayedHistories   = 102
)

var (
	//go:embed catalog.json
	catalogJSON []byte

	requiredTestReceipts = [...]string{
		"TestAdmittedTransportsReconstructEveryVerdict",
		"TestAdmittedTransportsReconstructEveryVerdict/hermetic-unsafe",
		"TestAdmittedTransportsReconstructEveryVerdict/hermetic-resume",
		"TestAdmittedTransportsReconstructEveryVerdict/hermetic-fenced",
		"TestAdmittedTransportsReconstructEveryVerdict/authenticated-unsafe",
		"TestAdmittedTransportsReconstructEveryVerdict/authenticated-resume",
		"TestAdmittedTransportsReconstructEveryVerdict/authenticated-fenced",
	}
)

type testEvent struct {
	Action string `json:"Action"`
	Test   string `json:"Test"`
}

func LoadCatalog() (presentation.Catalog, error) {
	return presentation.DecodeJSON(bytes.Clone(catalogJSON))
}

func RequiredTestReceipts() []string {
	return append([]string(nil), requiredTestReceipts[:]...)
}

func VerifyTestReceipts(reader io.Reader) error {
	scanner := bufio.NewScanner(io.LimitReader(reader, maxReceiptBytes+1))
	scanner.Buffer(make([]byte, 64*1024), maxReceiptLineBytes)
	passed := make(map[string]bool, len(requiredTestReceipts))
	required := make(map[string]bool, len(requiredTestReceipts))
	for _, name := range requiredTestReceipts {
		required[name] = true
	}
	total := 0
	for scanner.Scan() {
		total += len(scanner.Bytes()) + 1
		if total > maxReceiptBytes {
			return fmt.Errorf("test receipt stream exceeds %d bytes", maxReceiptBytes)
		}
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode go test receipt: %w", err)
		}
		if !required[event.Test] {
			continue
		}
		switch event.Action {
		case "pass":
			passed[event.Test] = true
		case "skip":
			return fmt.Errorf("required audit %q was skipped", event.Test)
		case "fail":
			return fmt.Errorf("required audit %q failed", event.Test)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read go test receipts: %w", err)
	}
	for _, name := range requiredTestReceipts {
		if !passed[name] {
			return fmt.Errorf("missing pass receipt for required audit %q", name)
		}
	}
	return nil
}

func WriteSummary(writer io.Writer, catalog presentation.Catalog) error {
	if err := presentation.Validate(catalog); err != nil {
		return fmt.Errorf("validate quickstart catalog: %w", err)
	}
	if len(catalog.Scenarios) != 1 {
		return errors.New("quickstart catalog must contain exactly one scenario")
	}
	scenario := catalog.Scenarios[0]
	if _, err := fmt.Fprintf(writer,
		"FIRST TRUSTWORTHY RECOVERY\n\n%s\nFault: %s\n\n",
		scenario.Title, scenario.FailureBoundary); err != nil {
		return fmt.Errorf("write summary heading: %w", err)
	}
	for _, episode := range scenario.Episodes {
		plural := "effects"
		if episode.Outcome.PhysicalEffectCount == 1 {
			plural = "effect"
		}
		if _, err := fmt.Fprintf(writer, "%-10s %-11s %d physical %s\n  history: %s :: %s\n",
			strings.ToUpper(string(episode.Variant)), string(episode.Verdict),
			episode.Outcome.PhysicalEffectCount, plural,
			episode.NativeHistory.Path, episode.NativeHistory.ArchiveMember); err != nil {
			return fmt.Errorf("write episode summary: %w", err)
		}
	}
	_, err := fmt.Fprintf(writer,
		"\n%d histories replayed by the credential-free transport audit.\n\n"+
			"Temporal: %s\nApplication: %s\nDestination: %s\nFalsifier: %s\n",
		ReplayedHistories, scenario.Responsibility.Temporal,
		scenario.Responsibility.Application, scenario.Responsibility.Destination,
		scenario.Falsifier)
	if err != nil {
		return fmt.Errorf("write responsibility summary: %w", err)
	}
	return nil
}
