package lab

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexDirectCoverageRunsEveryPortableRecoveryGate(t *testing.T) {
	command := exec.Command("make", "-n", "coverage-codex-direct")
	command.Dir = codexDirectRepositoryRoot(t)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run Codex direct coverage: %v\n%s", err, output)
	}
	text := string(output)
	for _, required := range []string{
		"CODEX_DIRECT_TRANSPORT_AUDIT=1",
		"CODEX_DIRECT_LIVE_TEST=1",
		"CODEX_DIRECT_EXTERNAL_COVERAGE=1",
		"CODEX_DIRECT_GOCOVERDIR=",
		"external-covdata",
		"TestAdmittedTransportsReconstructEveryVerdict",
		"for mode in unsafe-fresh explicit-thread-resume application-fenced; do",
		`-run "^TestLiveHermeticRecoveryMatrix/$mode$"`,
		"coverage.codex-direct.live.unsafe-fresh.out",
		"coverage.codex-direct.live.explicit-thread-resume.out",
		"coverage.codex-direct.live.application-fenced.out",
		"go tool covdata textfmt",
		"-pkg=github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab",
		"Codex child-process coverage is empty",
		"cmd/covermerge",
		"Codex direct lab coverage",
		"Codex evidence transport coverage",
		".coverage.codex-direct.XXXXXX",
		`coverage_directory=$(cd "$coverage_directory" && pwd -P)`,
		"-json -timeout 12m",
		`\"Action\":\"skip\"`,
		`\"Action\":\"pass\"`,
		"mv \"$coverage_directory/coverage.codex-direct.out\" coverage.codex-direct.out",
		"mv \"$coverage_directory/coverage.codex-direct-transport.out\" coverage.codex-direct-transport.out",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Codex direct coverage omits %q\n%s", required, text)
		}
	}
	if strings.Contains(text, "TOPOLOGY_PILOT_V5_REGRESSION=1") {
		t.Fatal("Codex direct coverage executes deferred controlled-host timing work")
	}
	for _, forbidden := range []string{
		"-coverprofile=coverage.codex-direct",
		"--output coverage.codex-direct.out",
		`CODEX_DIRECT_GOCOVERDIR="$coverage_directory"`,
		"| tee",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Codex direct coverage publishes or mixes staging data through %q", forbidden)
		}
	}
	if !strings.Contains(text, `CODEX_DIRECT_GOCOVERDIR="$coverage_directory/external-covdata"`) ||
		strings.Count(text, "if ! CODEX_DIRECT_") != 2 {
		t.Fatal("Codex coverage does not use CWD-independent child coverage and fail-preserving test receipts")
	}
	firstPublish := strings.Index(text, `mv "$coverage_directory/coverage.codex-direct.out"`)
	transportGate := strings.Index(text, "Codex evidence transport coverage")
	if firstPublish < 0 || transportGate < 0 || firstPublish < transportGate {
		t.Fatal("Codex direct coverage publishes before both coverage gates pass")
	}
	if got := strings.Count(text, `-run "^TestLiveHermeticRecoveryMatrix/$mode$"`); got != 1 {
		t.Fatalf("Codex direct coverage has %d live-loop run selectors, want 1", got)
	}

	mixed := exec.Command("make", "-n", "-j2", "coverage", "coverage-codex-direct")
	mixed.Dir = codexDirectRepositoryRoot(t)
	mixedOutput, err := mixed.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run mixed coverage goals: %v\n%s", err, mixedOutput)
	}
	if got := strings.Count(string(mixedOutput), `-coverprofile="$coverage_directory/coverage.codex-direct.base.out"`); got != 1 {
		t.Fatalf("mixed coverage goals schedule %d Codex base writers, want 1", got)
	}

	makefile, err := os.ReadFile(filepath.Join(codexDirectRepositoryRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(makefile), ".NOTPARALLEL: coverage-codex-direct") {
		t.Fatal("Codex direct coverage is not serialized")
	}
	for _, assignment := range []string{
		"override CODEX_DIRECT_COVERPKG :=",
		"override CODEX_DIRECT_COVER_IMPORT :=",
		"override CODEX_DIRECT_LIVE_MODES :=",
	} {
		if !strings.Contains(string(makefile), assignment) {
			t.Fatalf("Codex coverage permits command-line override of %q", assignment)
		}
	}
	for _, validation := range []string{
		`-v package="$(CODEX_DIRECT_COVER_IMPORT)/"`,
		`index($$0, package) == 1 { blocks++; next }`,
		`{ invalid = 1; exit } END { exit invalid || blocks == 0 }`,
	} {
		if !strings.Contains(string(makefile), validation) {
			t.Fatalf("Codex child coverage omits exact lab-block validation %q", validation)
		}
	}

	overridden := exec.Command("make", "-n", "coverage-codex-direct",
		"CODEX_DIRECT_COVERPKG=untrusted-cover-package",
		"CODEX_DIRECT_COVER_IMPORT=untrusted-cover-import",
		"CODEX_DIRECT_LIVE_MODES=untrusted-live-mode")
	overridden.Dir = codexDirectRepositoryRoot(t)
	overriddenOutput, err := overridden.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run overridden Codex coverage: %v\n%s", err, overriddenOutput)
	}
	if strings.Contains(string(overriddenOutput), "untrusted-") {
		t.Fatalf("Codex coverage accepted command-line contract override\n%s", overriddenOutput)
	}
}

func TestCodexDirectBuildAndCleanCoverEveryProducedArtifact(t *testing.T) {
	root := codexDirectRepositoryRoot(t)
	build := exec.Command("make", "-n", "codex-direct")
	build.Dir = root
	buildOutput, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run Codex direct build: %v\n%s", err, buildOutput)
	}
	for _, binary := range []string{
		"codex-direct-experiment",
		"codex-direct-worker",
		"codex-direct-effect",
		"codex-direct-launcher",
		"codex-direct-hermetic-codex",
		"codex-direct-evidence-audit",
		"codex-direct-evidence-transport",
	} {
		if !strings.Contains(string(buildOutput), binary) {
			t.Fatalf("Codex direct build omits %q", binary)
		}
	}

	clean := exec.Command("make", "-n", "clean")
	clean.Dir = root
	cleanOutput, err := clean.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run clean: %v\n%s", err, cleanOutput)
	}
	for _, artifact := range []string{
		".coverage.codex-direct.*",
		"coverage.codex-direct.out",
		"coverage.codex-direct-transport.out",
	} {
		if !strings.Contains(string(cleanOutput), artifact) {
			t.Fatalf("clean omits %q", artifact)
		}
	}
}

func TestCodexDirectCoverageDoesNotMaskPackageFailureAfterSubtestPass(t *testing.T) {
	directory := t.TempDir()
	goScript := `#!/bin/sh
profile=
json=0
for argument in "$@"; do
	case "$argument" in
		-coverprofile=*) profile=${argument#-coverprofile=} ;;
		-json) json=1 ;;
	esac
done
if [ "$json" -eq 0 ]; then
	printf 'mode: atomic\n' > "$profile"
	exit 0
fi
printf '%s\n' '{"Action":"pass","Test":"TestAdmittedTransportsReconstructEveryVerdict"}'
exit 91
`
	for name, contents := range map[string]string{"go": goScript, "temporal": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"coverage.codex-direct.out", "coverage.codex-direct-transport.out"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("preserved\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("make", "-f", filepath.Join(codexDirectRepositoryRoot(t), "Makefile"), "coverage-codex-direct")
	command.Dir = directory
	command.Env = append(os.Environ(), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), `"Action":"pass"`) {
		t.Fatalf("package failure after subtest PASS was masked: %v\n%s", err, output)
	}
	for _, name := range []string{"coverage.codex-direct.out", "coverage.codex-direct-transport.out"} {
		contents, readErr := os.ReadFile(filepath.Join(directory, name))
		if readErr != nil || string(contents) != "preserved\n" {
			t.Fatalf("failed coverage replaced %s: %v, %q", name, readErr, contents)
		}
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".coverage.codex-direct.*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("failed coverage left staging behind: %v, %v", err, matches)
	}
}

func TestCodexDirectCoverageRejectsExternalProfileWithoutOnlyLabBlocks(t *testing.T) {
	const goScript = `#!/bin/sh
set -eu
write_profile() {
	printf 'mode: atomic\n' > "$1"
	printf '%s\n' 'github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab/activity.go:56.106,57.42 1 1' >> "$1"
}
case "${1:-}" in
test)
	profile=
	json=0
	for argument in "$@"; do
		case "$argument" in
		-coverprofile=*) profile=${argument#-coverprofile=} ;;
		-json) json=1 ;;
		esac
	done
	write_profile "$profile"
	if [ "$json" -eq 1 ]; then
		for test in TestAdmittedTransportsReconstructEveryVerdict TestLiveHermeticRecoveryMatrix/unsafe-fresh TestLiveHermeticRecoveryMatrix/explicit-thread-resume TestLiveHermeticRecoveryMatrix/application-fenced; do
			printf '{"Action":"pass","Test":"%s"}\n' "$test"
		done
	fi
	;;
tool)
	case "${2:-}" in
	covdata)
		output=
		for argument in "$@"; do case "$argument" in -o=*) output=${argument#-o=} ;; esac; done
		printf 'mode: atomic\n' > "$output"
		case "${FAKE_EXTERNAL_PROFILE:-}" in
		command|mixed) printf '%s\n' 'github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/cmd/worker/main.go:1.1,1.2 1 1' >> "$output" ;;
		esac
		if [ "${FAKE_EXTERNAL_PROFILE:-}" = mixed ]; then
			printf '%s\n' 'github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab/activity.go:56.106,57.42 1 1' >> "$output"
		fi
		;;
	cover) printf 'total:\t(statements)\t80.1%%\n' ;;
	esac
	;;
run)
	output=
	previous=
	for argument in "$@"; do
		if [ "$previous" = --output ]; then output=$argument; break; fi
		previous=$argument
	done
	write_profile "$output"
	;;
esac
`
	for _, profile := range []string{"empty", "command", "mixed"} {
		t.Run(profile, func(t *testing.T) {
			directory := t.TempDir()
			for name, contents := range map[string]string{"go": goScript, "temporal": "#!/bin/sh\nexit 0\n"} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte(contents), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"coverage.codex-direct.out", "coverage.codex-direct-transport.out"} {
				if err := os.WriteFile(filepath.Join(directory, name), []byte("preserved\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("make", "-f", filepath.Join(codexDirectRepositoryRoot(t), "Makefile"), "coverage-codex-direct")
			command.Dir = directory
			command.Env = append(os.Environ(),
				"PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_EXTERNAL_PROFILE="+profile)
			output, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "lacks exact lab blocks") {
				t.Fatalf("external %s profile was admitted: %v\n%s", profile, err, output)
			}
			for _, name := range []string{"coverage.codex-direct.out", "coverage.codex-direct-transport.out"} {
				contents, readErr := os.ReadFile(filepath.Join(directory, name))
				if readErr != nil || string(contents) != "preserved\n" {
					t.Fatalf("rejected profile replaced %s: %v, %q", name, readErr, contents)
				}
			}
		})
	}
}
