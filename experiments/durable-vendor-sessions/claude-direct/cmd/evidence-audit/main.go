package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/claude-direct/internal/lab"
)

type auditOptions struct {
	root   string
	output string
	mode   string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var report any
	switch options.mode {
	case "direct":
		direct, auditErr := lab.AuditDirectEvidence(ctx, options.root)
		err = auditErr
		if err == nil {
			err = lab.WriteDirectEvidenceAudit(options.output, direct)
		}
		report = direct
	case "fenced":
		fenced, auditErr := lab.AuditFencedEvidence(ctx, options.root)
		err = auditErr
		if err == nil {
			err = lab.WriteFencedEvidenceAudit(options.output, fenced)
		}
		report = fenced
	case "resume-only":
		resume, auditErr := lab.AuditResumeEvidence(ctx, options.root)
		err = auditErr
		if err == nil {
			err = lab.WriteResumeEvidenceAudit(options.output, resume)
		}
		report = resume
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string) (auditOptions, error) {
	flags := flag.NewFlagSet("claude-direct-evidence-audit", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options auditOptions
	flags.StringVar(&options.root, "root", "", "sealed fenced experiment evidence root")
	flags.StringVar(&options.output, "output", "", "new append-only audit report path")
	flags.StringVar(&options.mode, "mode", "", "audit mode: direct, fenced, or resume-only")
	if err := flags.Parse(args); err != nil {
		return auditOptions{}, fmt.Errorf("parse audit flags: %w", err)
	}
	if flags.NArg() != 0 || options.root == "" || options.output == "" ||
		options.mode != "direct" && options.mode != "fenced" && options.mode != "resume-only" {
		return auditOptions{}, errors.New("mode, evidence root, and new audit output path are required; positional arguments are not accepted")
	}
	return options, nil
}
