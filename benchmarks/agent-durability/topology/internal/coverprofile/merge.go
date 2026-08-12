package coverprofile

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// NamedReader identifies an input profile in validation errors.
type NamedReader struct {
	Name   string
	Reader io.Reader
}

type block struct {
	statements uint64
	count      uint64
}

// Merge combines profiles produced with the same Go coverage mode. Execution
// counts are added for identical source blocks so no input profile can erase
// coverage established by another gate.
func Merge(output io.Writer, profiles []NamedReader) error {
	if len(profiles) == 0 {
		return fmt.Errorf("merge coverage profiles: no inputs")
	}
	mode := ""
	blocks := make(map[string]block)
	fileLayouts := make(map[string]map[string]struct{})
	for _, profile := range profiles {
		if profile.Reader == nil {
			return fmt.Errorf("merge coverage profile %q: nil reader", profile.Name)
		}
		scanner := bufio.NewScanner(profile.Reader)
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read coverage profile %q: %w", profile.Name, err)
			}
			return fmt.Errorf("read coverage profile %q: missing coverage mode", profile.Name)
		}
		profileMode := scanner.Text()
		if profileMode != "mode: atomic" && profileMode != "mode: count" && profileMode != "mode: set" {
			return fmt.Errorf("read coverage profile %q: invalid coverage mode %q", profile.Name, profileMode)
		}
		if mode == "" {
			mode = profileMode
		} else if mode != profileMode {
			return fmt.Errorf("merge coverage profile %q: coverage mode %q does not match %q", profile.Name, profileMode, mode)
		}
		profileLayouts := make(map[string]map[string]struct{})
		line := 1
		for scanner.Scan() {
			line++
			if strings.TrimSpace(scanner.Text()) == "" {
				continue
			}
			fields := strings.Fields(scanner.Text())
			if len(fields) != 3 {
				return fmt.Errorf("read coverage profile %q line %d: malformed block", profile.Name, line)
			}
			statements, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return fmt.Errorf("read coverage profile %q line %d statement count: %w", profile.Name, line, err)
			}
			count, err := strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return fmt.Errorf("read coverage profile %q line %d execution count: %w", profile.Name, line, err)
			}
			file, err := sourceFile(fields[0], statements)
			if err != nil {
				return fmt.Errorf("read coverage profile %q line %d: %w", profile.Name, line, err)
			}
			existing, exists := blocks[fields[0]]
			if exists && existing.statements != statements {
				return fmt.Errorf("merge coverage profile %q line %d: statement count for %s is %d, previously %d", profile.Name, line, fields[0], statements, existing.statements)
			}
			if ^uint64(0)-existing.count < count {
				return fmt.Errorf("merge coverage profile %q line %d: execution count overflow for %s", profile.Name, line, fields[0])
			}
			blocks[fields[0]] = block{statements: statements, count: existing.count + count}
			if profileLayouts[file] == nil {
				profileLayouts[file] = make(map[string]struct{})
			}
			profileLayouts[file][fields[0]] = struct{}{}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read coverage profile %q: %w", profile.Name, err)
		}
		for file, layout := range profileLayouts {
			if existing, found := fileLayouts[file]; found && !sameLayout(existing, layout) {
				return fmt.Errorf("merge coverage profile %q: block layout for %s does not match earlier profiles", profile.Name, file)
			}
			fileLayouts[file] = layout
		}
	}
	keys := make([]string, 0, len(blocks))
	for key := range blocks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if _, err := fmt.Fprintln(output, mode); err != nil {
		return fmt.Errorf("write merged coverage mode: %w", err)
	}
	for _, key := range keys {
		block := blocks[key]
		if _, err := fmt.Fprintf(output, "%s %d %d\n", key, block.statements, block.count); err != nil {
			return fmt.Errorf("write merged coverage block %s: %w", key, err)
		}
	}
	return nil
}

func sourceFile(blockKey string, statements uint64) (string, error) {
	separator := strings.LastIndexByte(blockKey, ':')
	if separator <= 0 {
		return "", fmt.Errorf("invalid coverage block %q", blockKey)
	}
	positions := strings.Split(blockKey[separator+1:], ",")
	if len(positions) != 2 {
		return "", fmt.Errorf("invalid coverage block %q", blockKey)
	}
	startLine, startColumn, err := sourcePosition(positions[0])
	if err != nil {
		return "", fmt.Errorf("invalid coverage block %q", blockKey)
	}
	endLine, endColumn, err := sourcePosition(positions[1])
	if err != nil || endLine < startLine || endLine == startLine && endColumn < startColumn {
		return "", fmt.Errorf("invalid coverage block %q", blockKey)
	}
	empty := endLine == startLine && endColumn == startColumn
	if empty != (statements == 0) {
		return "", fmt.Errorf("invalid coverage block %q", blockKey)
	}
	return blockKey[:separator], nil
}

func sourcePosition(value string) (uint64, uint64, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid source position")
	}
	line, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || line == 0 {
		return 0, 0, fmt.Errorf("invalid source line")
	}
	column, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || column == 0 {
		return 0, 0, fmt.Errorf("invalid source column")
	}
	return line, column, nil
}

func sameLayout(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, found := right[key]; !found {
			return false
		}
	}
	return true
}
