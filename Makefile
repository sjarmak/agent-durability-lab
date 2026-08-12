.PHONY: build claude-direct-evidence-transport codex-direct test test-live check-postgres-service check-publication-v2-config check-topology-controlled-host publication-v2 coding-agent-conformance coverage-coding-agent-conformance coverage-codex-direct coverage-topology coverage-topology-timing topology-semantics-conformance topology-recovery-conformance topology-matrix-conformance topology-pilot test-system-adapters test-temporal-native coverage coverage-system-adapters evidence-temporal-native evidence-claude-direct package-claude-direct-evidence verify-claude-direct-evidence restore-claude-direct-evidence clean

.NOTPARALLEL: coverage-codex-direct coverage-topology-timing

PUBLICATION_DEADLINE ?= 2h
TOPOLOGY_PILOT_DEADLINE ?= 8h
CLAUDE_DIRECT_EVIDENCE_SOURCE ?= $(CURDIR)/experiments/durable-vendor-sessions/claude-direct/evidence
CLAUDE_DIRECT_EVIDENCE_LINEAGE ?= $(CURDIR)/experiments/durable-vendor-sessions/claude-direct/transport/claude-direct-lineage.json
AGENT_DURABILITY_COVER_TEST_PACKAGES := ./benchmarks/agent-durability ./benchmarks/agent-durability/calibration ./benchmarks/agent-durability/cmd/calibrate ./benchmarks/agent-durability/cmd/live-common ./benchmarks/agent-durability/cmd/publication-report ./benchmarks/agent-durability/cmd/system-suite ./benchmarks/agent-durability/evidence ./benchmarks/agent-durability/livecommon ./benchmarks/agent-durability/oracle ./benchmarks/agent-durability/protocol ./benchmarks/agent-durability/systemsuite
CODING_AGENT_CONFORMANCE_COVERPKG := ./benchmarks/agent-durability/conformance/evidence,./benchmarks/agent-durability/conformance/oracle,./benchmarks/agent-durability/conformance/profile,./benchmarks/agent-durability/conformance/cmd/conformance
CODING_AGENT_CONFORMANCE_TEST_PACKAGES := ./benchmarks/agent-durability/conformance/evidence ./benchmarks/agent-durability/conformance/oracle ./benchmarks/agent-durability/conformance/profile ./benchmarks/agent-durability/conformance/cmd/conformance
override CODEX_DIRECT_COVERPKG := ./experiments/durable-vendor-sessions/codex-direct/internal/lab
override CODEX_DIRECT_COVER_IMPORT := github.com/sjarmak/temporal_projects/experiments/durable-vendor-sessions/codex-direct/internal/lab
override CODEX_DIRECT_LIVE_MODES := unsafe-fresh explicit-thread-resume application-fenced
TOPOLOGY_COVERPKG := ./benchmarks/agent-durability/topology/agent,./benchmarks/agent-durability/topology/cmd/covermerge,./benchmarks/agent-durability/topology/evidence,./benchmarks/agent-durability/topology/internal/coverprofile,./benchmarks/agent-durability/topology/internal/sealedfs,./benchmarks/agent-durability/topology/matrix,./benchmarks/agent-durability/topology/oracle,./benchmarks/agent-durability/topology/protocol,./benchmarks/agent-durability/topology/runner,./benchmarks/agent-durability/topology/semantics
TOPOLOGY_COVER_TEST_PACKAGES := ./benchmarks/agent-durability/topology/agent ./benchmarks/agent-durability/topology/cmd/covermerge ./benchmarks/agent-durability/topology/cmd/matrix-conformance ./benchmarks/agent-durability/topology/cmd/pilot ./benchmarks/agent-durability/topology/cmd/semantics-conformance ./benchmarks/agent-durability/topology/evidence ./benchmarks/agent-durability/topology/internal/coverprofile ./benchmarks/agent-durability/topology/internal/sealedfs ./benchmarks/agent-durability/topology/internal/testfixture ./benchmarks/agent-durability/topology/matrix ./benchmarks/agent-durability/topology/oracle ./benchmarks/agent-durability/topology/protocol ./benchmarks/agent-durability/topology/runner
TOPOLOGY_COVER_INTEGRATIONS := TestTemporalExecutorRecoversJoinAcrossBothTopologyArms TestTemporalExecutorCoversFrozenSemanticsCasesAndBoundaries TestTemporalExecutorCoversFrozenRecoveryCasesAndBoundaries TestTemporalExecutorRecoveryScaleDoesNotDeadlockAdmission TestTemporalExecutorUnsafeQueuedChildScaleClosesHeldBarriers
TOPOLOGY_COVER_SHAPES := unfaulted-direct-supersession-32 unfaulted-child-outage-128 protected-child-outage-128 protected-child-silent-progress-8
TOPOLOGY_COVER_BASE_PROFILES := coverage.topology.packages.out coverage.topology.semantics-unit.out coverage.topology.pilot-audit.out $(addprefix coverage.topology.,$(addsuffix .out,$(TOPOLOGY_COVER_INTEGRATIONS)))
TOPOLOGY_TIMING_PROFILES := $(addprefix coverage.topology.v5.,$(addsuffix .out,$(TOPOLOGY_COVER_SHAPES)))

build: claude-direct-evidence-transport codex-direct
	mkdir -p bin
	go build -o bin/lab-worker ./cmd/worker
	go build -o bin/agent-simulator ./cmd/agent-simulator
	go build -o bin/worker-death-experiment ./experiments/worker-death/cmd/experiment
	go build -o bin/activity-completion-identity-experiment ./experiments/activity-completion-identity/cmd/experiment
	go build -o bin/external-effect-worker ./experiments/external-effects/cmd/worker
	go build -o bin/external-effect-experiment ./experiments/external-effects/cmd/experiment
	go build -o bin/cancellation-experiment ./experiments/cancellation/cmd/experiment
	go build -o bin/agent-durability-calibrate ./benchmarks/agent-durability/cmd/calibrate
	go build -o bin/agent-durability-live-common ./benchmarks/agent-durability/cmd/live-common
	go build -o bin/agent-durability-v2-calibrate ./benchmarks/agent-durability/v2/cmd/calibrate
	go build -o bin/agent-durability-v2-aba-client ./benchmarks/agent-durability/v2/cmd/aba-client
	go build -o bin/agent-durability-v2-live-aba ./benchmarks/agent-durability/v2/cmd/live-aba
	go build -o bin/agent-durability-v2-system-suite ./benchmarks/agent-durability/v2/cmd/system-suite
	go build -o bin/agent-durability-v2-publication ./benchmarks/agent-durability/v2/cmd/publication
	go build -o bin/agent-durability-v2-publication-report ./benchmarks/agent-durability/cmd/publication-report
	go build -o bin/agent-durability-v1-system-suite ./benchmarks/agent-durability/cmd/system-suite
	go build -o bin/topology-semantics-conformance ./benchmarks/agent-durability/topology/cmd/semantics-conformance
	go build -o bin/topology-matrix-conformance ./benchmarks/agent-durability/topology/cmd/matrix-conformance
	go build -o bin/topology-pilot ./benchmarks/agent-durability/topology/cmd/pilot
	go build -o bin/temporal-native-evidence ./experiments/durable-vendor-sessions/temporal-native/cmd/temporal-native-evidence
	go build -o bin/claude-direct-worker ./experiments/durable-vendor-sessions/claude-direct/cmd/worker
	go build -o bin/claude-direct-effect ./experiments/durable-vendor-sessions/claude-direct/cmd/controlled-effect
	go build -o bin/claude-direct-launcher ./experiments/durable-vendor-sessions/claude-direct/cmd/claude-launcher
	go build -o bin/claude-direct-experiment ./experiments/durable-vendor-sessions/claude-direct/cmd/experiment
	go build -o bin/claude-direct-evidence-audit ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-audit
	go build -o bin/claude-direct-hermetic-claude ./experiments/durable-vendor-sessions/claude-direct/cmd/hermetic-claude

claude-direct-evidence-transport:
	mkdir -p bin
	go build -o bin/claude-direct-evidence-transport ./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport

codex-direct:
	mkdir -p bin
	go build -trimpath -o bin/codex-direct-experiment ./experiments/durable-vendor-sessions/codex-direct/cmd/experiment
	go build -trimpath -o bin/codex-direct-worker ./experiments/durable-vendor-sessions/codex-direct/cmd/worker
	go build -trimpath -o bin/codex-direct-effect ./experiments/durable-vendor-sessions/codex-direct/cmd/controlled-effect
	go build -trimpath -o bin/codex-direct-launcher ./experiments/durable-vendor-sessions/codex-direct/cmd/codex-launcher
	go build -trimpath -o bin/codex-direct-hermetic-codex ./experiments/durable-vendor-sessions/codex-direct/cmd/hermetic-codex
	go build -trimpath -o bin/codex-direct-evidence-audit ./experiments/durable-vendor-sessions/codex-direct/cmd/evidence-audit
	go build -trimpath -o bin/codex-direct-evidence-transport ./experiments/durable-vendor-sessions/codex-direct/cmd/evidence-transport

test:
	go test -race ./...

test-live:
	go test -race -v -run TestLiveTemporal -timeout 12m \
		./experiments/worker-death/internal/lab \
		./experiments/activity-completion-identity/internal/lab \
		./experiments/external-effects/internal/lab \
		./experiments/cancellation/internal/lab
	go test -race -v -run TestRunExperimentWithFakeClaudeProvesUnsafeRetryViolation -timeout 8m \
		./experiments/durable-vendor-sessions/claude-direct/internal/lab

check-postgres-service:
	@case "$(POSTGRES_SERVICE)" in ""|*[!A-Za-z0-9_.-]*) echo "POSTGRES_SERVICE must contain only letters, digits, dot, underscore, or hyphen"; exit 1 ;; esac

check-publication-v2-config: check-postgres-service
	@case "$(PHASE)" in pilot|publication) ;; *) echo "PHASE must be pilot or publication"; exit 1 ;; esac
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_WORK_ROOT)" || (echo "TEMPORAL_WORK_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)

publication-v2: check-publication-v2-config build
	bin/agent-durability-v2-publication \
		--phase "$(PHASE)" \
		--root "$(EVIDENCE_ROOT)" \
		--work-root "$(TEMPORAL_WORK_ROOT)" \
		--temporal-path "$(TEMPORAL_CLI_PATH)" \
		--postgres-dsn "service=$(POSTGRES_SERVICE)" \
		--deadline "$(PUBLICATION_DEADLINE)"

coding-agent-conformance:
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required and must not exist"; exit 1)
	@test ! -e "$(EVIDENCE_ROOT)" && test ! -L "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT must not already exist"; exit 1)
	mkdir -p bin
	go build -trimpath -o bin/coding-agent-conformance ./benchmarks/agent-durability/conformance/cmd/conformance
	bin/coding-agent-conformance \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--source-root "$(CURDIR)" \
		--schema-root "$(CURDIR)/specs/coding-agent-durability/v1/schema"

topology-semantics-conformance:
	mkdir -p bin
	go build -trimpath -o bin/agent-simulator ./cmd/agent-simulator
	go build -trimpath -o bin/topology-semantics-conformance ./benchmarks/agent-durability/topology/cmd/semantics-conformance
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_WORK_ROOT)" || (echo "TEMPORAL_WORK_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	bin/topology-semantics-conformance \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--work-root "$(TEMPORAL_WORK_ROOT)" \
		--temporal-path "$(TEMPORAL_CLI_PATH)" \
		--agent-binary "$(CURDIR)/bin/agent-simulator"

topology-recovery-conformance:
	mkdir -p bin
	go build -trimpath -o bin/agent-simulator ./cmd/agent-simulator
	go build -trimpath -o bin/topology-semantics-conformance ./benchmarks/agent-durability/topology/cmd/semantics-conformance
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_WORK_ROOT)" || (echo "TEMPORAL_WORK_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	bin/topology-semantics-conformance \
		--suite recovery \
		--fanout 32 \
		--deadline 40m \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--work-root "$(TEMPORAL_WORK_ROOT)" \
		--temporal-path "$(TEMPORAL_CLI_PATH)" \
		--agent-binary "$(CURDIR)/bin/agent-simulator"

topology-matrix-conformance:
	mkdir -p bin
	go build -trimpath -o bin/agent-simulator ./cmd/agent-simulator
	go build -trimpath -o bin/topology-matrix-conformance ./benchmarks/agent-durability/topology/cmd/matrix-conformance
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_WORK_ROOT)" || (echo "TEMPORAL_WORK_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	bin/topology-matrix-conformance \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--work-root "$(TEMPORAL_WORK_ROOT)" \
		--preregistration "$(CURDIR)/benchmarks/agent-durability/topology-preregistration-v1.json" \
		--temporal-path "$(TEMPORAL_CLI_PATH)" \
		--agent-binary "$(CURDIR)/bin/agent-simulator"

topology-pilot:
	mkdir -p bin
	go build -trimpath -o bin/agent-simulator ./cmd/agent-simulator
	go build -trimpath -o bin/topology-pilot ./benchmarks/agent-durability/topology/cmd/pilot
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_WORK_ROOT)" || (echo "TEMPORAL_WORK_ROOT is required"; exit 1)
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	bin/topology-pilot \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--work-root "$(TEMPORAL_WORK_ROOT)" \
		--preregistration "$(CURDIR)/benchmarks/agent-durability/topology-preregistration-v1.json" \
		--contract "$(CURDIR)/benchmarks/agent-durability/topology-contract-v1.json" \
		--temporal-path "$(TEMPORAL_CLI_PATH)" \
		--agent-binary "$(CURDIR)/bin/agent-simulator" \
		--source-root "$(CURDIR)" \
		--deadline "$(TOPOLOGY_PILOT_DEADLINE)"

test-system-adapters: check-postgres-service
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	POSTGRES_DSN="service=$(POSTGRES_SERVICE)" go test -race -v -run TestLive -count=1 \
		./benchmarks/agent-durability/v2/temporaladapter \
		./benchmarks/agent-durability/v2/postgresadapter

test-temporal-native:
	cd experiments/durable-vendor-sessions/temporal-native && uv run pytest -q

evidence-temporal-native:
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	uv run --project experiments/durable-vendor-sessions/temporal-native \
		python -m temporal_native.run_trials --evidence-root "$(EVIDENCE_ROOT)" --trials 3

evidence-claude-direct: build
	@test -n "$(EVIDENCE_ROOT)" || (echo "EVIDENCE_ROOT is required"; exit 1)
	bin/claude-direct-experiment \
		--evidence-root "$(EVIDENCE_ROOT)" \
		--temporal-binary "$$(command -v temporal)" \
		--worker-binary "$(CURDIR)/bin/claude-direct-worker" \
		--effect-binary "$(CURDIR)/bin/claude-direct-effect" \
		--launcher-binary "$(CURDIR)/bin/claude-direct-launcher" \
		--claude-binary "$${CLAUDE_BINARY:-$$(command -v claude)}" \
		--model "$${CLAUDE_MODEL:-haiku}" \
		--max-budget-usd "$${CLAUDE_MAX_BUDGET_USD:-0.25}" \
		--max-turns "$${CLAUDE_MAX_TURNS:-2}" \
		--recovery-mode "$${CLAUDE_RECOVERY_MODE:-unsafe-fresh}" \
		--trials 3

package-claude-direct-evidence: claude-direct-evidence-transport
	@test -n "$(CLAUDE_DIRECT_TRANSPORT_ROOT)" || (echo "CLAUDE_DIRECT_TRANSPORT_ROOT is required and must not exist"; exit 1)
	bin/claude-direct-evidence-transport package \
		--source "$(CLAUDE_DIRECT_EVIDENCE_SOURCE)" \
		--lineage "$(CLAUDE_DIRECT_EVIDENCE_LINEAGE)" \
		--output "$(CLAUDE_DIRECT_TRANSPORT_ROOT)"

verify-claude-direct-evidence: claude-direct-evidence-transport
	@test -n "$(CLAUDE_DIRECT_TRANSPORT_ROOT)" || (echo "CLAUDE_DIRECT_TRANSPORT_ROOT is required"; exit 1)
	bin/claude-direct-evidence-transport verify --transport "$(CLAUDE_DIRECT_TRANSPORT_ROOT)"

restore-claude-direct-evidence: claude-direct-evidence-transport
	@test -n "$(CLAUDE_DIRECT_TRANSPORT_ROOT)" || (echo "CLAUDE_DIRECT_TRANSPORT_ROOT is required"; exit 1)
	@test -n "$(CLAUDE_DIRECT_RESTORE_ROOT)" || (echo "CLAUDE_DIRECT_RESTORE_ROOT is required and must not exist"; exit 1)
	bin/claude-direct-evidence-transport restore \
		--transport "$(CLAUDE_DIRECT_TRANSPORT_ROOT)" \
		--output "$(CLAUDE_DIRECT_RESTORE_ROOT)"

coverage-coding-agent-conformance:
	go test -race -count=1 -coverpkg=$(CODING_AGENT_CONFORMANCE_COVERPKG) \
		-coverprofile=coverage.coding-agent-conformance.out $(CODING_AGENT_CONFORMANCE_TEST_PACKAGES)
	go tool cover -func=coverage.coding-agent-conformance.out
	@conformance_coverage=$$(go tool cover -func=coverage.coding-agent-conformance.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$conformance_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "coding-agent conformance coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

coverage: coverage-coding-agent-conformance coverage-codex-direct coverage-topology
	go test -race -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out
	@core_coverage=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$core_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "core coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.completion.out ./experiments/activity-completion-identity/internal/lab
	go tool cover -func=coverage.completion.out
	@lab_coverage=$$(go tool cover -func=coverage.completion.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "completion identity coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.external-effects.out ./experiments/external-effects/internal/lab
	go tool cover -func=coverage.external-effects.out
	@lab_coverage=$$(go tool cover -func=coverage.external-effects.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "external effects coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.cancellation.out ./experiments/cancellation/internal/lab
	go tool cover -func=coverage.cancellation.out
	@lab_coverage=$$(go tool cover -func=coverage.cancellation.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "cancellation coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverpkg=./benchmarks/agent-durability/calibration,./benchmarks/agent-durability/evidence,./benchmarks/agent-durability/livecommon,./benchmarks/agent-durability/oracle,./benchmarks/agent-durability/protocol -coverprofile=coverage.agent-durability.out $(AGENT_DURABILITY_COVER_TEST_PACKAGES)
	go tool cover -func=coverage.agent-durability.out
	@harness_coverage=$$(go tool cover -func=coverage.agent-durability.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$harness_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "agent durability harness coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.claude-direct.base.out ./experiments/durable-vendor-sessions/claude-direct/internal/lab
	CLAUDE_DIRECT_TRANSPORT_AUDIT=1 go test -race -count=1 \
		-coverpkg=./experiments/durable-vendor-sessions/claude-direct/internal/lab \
		-coverprofile=coverage.claude-direct.audit.out \
		./experiments/durable-vendor-sessions/claude-direct/internal/lab \
		-run '^TestAdmittedTransportsReconstructEveryVerdict$$'
	go run ./benchmarks/agent-durability/topology/cmd/covermerge \
		--output coverage.claude-direct.out coverage.claude-direct.base.out coverage.claude-direct.audit.out
	go tool cover -func=coverage.claude-direct.out
	@lab_coverage=$$(go tool cover -func=coverage.claude-direct.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Claude direct lab coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.claude-direct-transport.out \
		./experiments/durable-vendor-sessions/claude-direct/transport \
		./experiments/durable-vendor-sessions/claude-direct/cmd/evidence-transport
	go tool cover -func=coverage.claude-direct-transport.out
	@transport_coverage=$$(go tool cover -func=coverage.claude-direct-transport.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$transport_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Claude evidence transport coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.temporal-native-adapter.out ./experiments/durable-vendor-sessions/temporal-native/evidenceadapter
	go tool cover -func=coverage.temporal-native-adapter.out
	@adapter_coverage=$$(go tool cover -func=coverage.temporal-native-adapter.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$adapter_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Temporal-native adapter coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	cd experiments/durable-vendor-sessions/temporal-native && \
		uv run pytest -q --cov=temporal_native --cov-branch --cov-fail-under=80
	go test -race -coverpkg=./benchmarks/agent-durability/v2/abalive,./benchmarks/agent-durability/v2/calibration,./benchmarks/agent-durability/v2/evidence,./benchmarks/agent-durability/v2/oracle,./benchmarks/agent-durability/v2/protocol -coverprofile=coverage.agent-durability-v2.out ./benchmarks/agent-durability/v2/...
	go tool cover -func=coverage.agent-durability-v2.out
	@v2_coverage=$$(go tool cover -func=coverage.agent-durability-v2.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$v2_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "agent durability v2 coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

coverage-codex-direct:
	@command -v temporal >/dev/null || { echo "Temporal CLI is required for Codex direct coverage"; exit 1; }
	@set -eu; coverage_directory=$$(mktemp -d .coverage.codex-direct.XXXXXX); \
		trap 'rm -rf "$$coverage_directory"' EXIT HUP INT TERM; \
		coverage_directory=$$(cd "$$coverage_directory" && pwd -P); \
		mkdir "$$coverage_directory/external-covdata"; \
		CODEX_DIRECT_LIVE_TEST= CODEX_DIRECT_TRANSPORT_AUDIT= CODEX_DIRECT_EXTERNAL_COVERAGE= CODEX_DIRECT_GOCOVERDIR= \
		go test -race -count=1 -covermode=atomic -coverpkg=$(CODEX_DIRECT_COVERPKG) \
			-coverprofile="$$coverage_directory/coverage.codex-direct.base.out" $(CODEX_DIRECT_COVERPKG); \
		if ! CODEX_DIRECT_LIVE_TEST= CODEX_DIRECT_TRANSPORT_AUDIT=1 CODEX_DIRECT_EXTERNAL_COVERAGE= CODEX_DIRECT_GOCOVERDIR= \
			go test -race -count=1 -json -timeout 12m -covermode=atomic \
				-coverpkg=$(CODEX_DIRECT_COVERPKG) \
				-coverprofile="$$coverage_directory/coverage.codex-direct.audit.out" \
				$(CODEX_DIRECT_COVERPKG) -run '^TestAdmittedTransportsReconstructEveryVerdict$$' \
				>"$$coverage_directory/audit.jsonl"; then \
			cat "$$coverage_directory/audit.jsonl"; exit 1; \
		fi; \
		awk -v test='TestAdmittedTransportsReconstructEveryVerdict' \
			'index($$0, "\"Action\":\"skip\"") { skip = 1 } index($$0, "\"Action\":\"pass\"") && index($$0, "\"Test\":\"" test "\"") { pass = 1 } END { exit !(pass && !skip) }' \
			"$$coverage_directory/audit.jsonl" || { echo 'Codex transport audit did not execute and pass without skips'; exit 1; }; \
		for mode in $(CODEX_DIRECT_LIVE_MODES); do \
			if ! CODEX_DIRECT_LIVE_TEST=1 CODEX_DIRECT_TRANSPORT_AUDIT= CODEX_DIRECT_EXTERNAL_COVERAGE=1 \
				CODEX_DIRECT_GOCOVERDIR="$$coverage_directory/external-covdata" \
				go test -race -count=1 -json -timeout 12m -covermode=atomic -coverpkg=$(CODEX_DIRECT_COVERPKG) \
					-coverprofile="$$coverage_directory/coverage.codex-direct.live.$$mode.out" \
					$(CODEX_DIRECT_COVERPKG) -run "^TestLiveHermeticRecoveryMatrix/$$mode$$" \
					>"$$coverage_directory/live.$$mode.jsonl"; then \
				cat "$$coverage_directory/live.$$mode.jsonl"; exit 1; \
			fi; \
			awk -v test="TestLiveHermeticRecoveryMatrix/$$mode" \
				'index($$0, "\"Action\":\"skip\"") { skip = 1 } index($$0, "\"Action\":\"pass\"") && index($$0, "\"Test\":\"" test "\"") { pass = 1 } END { exit !(pass && !skip) }' \
				"$$coverage_directory/live.$$mode.jsonl" || { echo "Codex live mode $$mode did not execute and pass without skips"; exit 1; }; \
		done; \
		go tool covdata textfmt -pkg=$(CODEX_DIRECT_COVER_IMPORT) \
			-i="$$coverage_directory/external-covdata" \
			-o="$$coverage_directory/coverage.codex-direct.external.out"; \
		test -s "$$coverage_directory/coverage.codex-direct.external.out" || { echo 'Codex child-process coverage is empty'; exit 1; }; \
		awk -v package="$(CODEX_DIRECT_COVER_IMPORT)/" \
			'NR == 1 { if ($$0 != "mode: atomic") exit 1; next } \
			 index($$0, package) == 1 { blocks++; next } \
			 { invalid = 1; exit } END { exit invalid || blocks == 0 }' \
			"$$coverage_directory/coverage.codex-direct.external.out" || { echo 'Codex child-process coverage lacks exact lab blocks'; exit 1; }; \
		go run ./benchmarks/agent-durability/topology/cmd/covermerge \
			--output "$$coverage_directory/coverage.codex-direct.out" \
			"$$coverage_directory/coverage.codex-direct.base.out" \
			"$$coverage_directory/coverage.codex-direct.audit.out" \
			"$$coverage_directory/coverage.codex-direct.live.unsafe-fresh.out" \
			"$$coverage_directory/coverage.codex-direct.live.explicit-thread-resume.out" \
			"$$coverage_directory/coverage.codex-direct.live.application-fenced.out" \
			"$$coverage_directory/coverage.codex-direct.external.out"; \
		go tool cover -func="$$coverage_directory/coverage.codex-direct.out"; \
		lab_coverage=$$(go tool cover -func="$$coverage_directory/coverage.codex-direct.out" | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
		awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Codex direct lab coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'; \
		CODEX_DIRECT_LIVE_TEST= CODEX_DIRECT_TRANSPORT_AUDIT= CODEX_DIRECT_EXTERNAL_COVERAGE= CODEX_DIRECT_GOCOVERDIR= \
		go test -race -count=1 -covermode=atomic \
			-coverprofile="$$coverage_directory/coverage.codex-direct-transport.out" \
			./experiments/durable-vendor-sessions/codex-direct/transport \
			./experiments/durable-vendor-sessions/codex-direct/cmd/evidence-transport; \
		go tool cover -func="$$coverage_directory/coverage.codex-direct-transport.out"; \
		transport_coverage=$$(go tool cover -func="$$coverage_directory/coverage.codex-direct-transport.out" | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
		awk -v coverage="$$transport_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Codex evidence transport coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'; \
		mv "$$coverage_directory/coverage.codex-direct.out" coverage.codex-direct.out; \
		mv "$$coverage_directory/coverage.codex-direct-transport.out" coverage.codex-direct-transport.out

coverage-topology:
	go test -race -count=1 -coverpkg=$(TOPOLOGY_COVERPKG) \
		-coverprofile=coverage.topology.packages.out $(TOPOLOGY_COVER_TEST_PACKAGES)
	go test -race -short -count=1 -coverpkg=$(TOPOLOGY_COVERPKG) \
		-coverprofile=coverage.topology.semantics-unit.out ./benchmarks/agent-durability/topology/semantics
	TOPOLOGY_PILOT_AUDIT_ROOT=$(CURDIR)/benchmarks/agent-durability/topology/evidence/pilot-20260810-v5 \
		go test -race -count=1 -coverpkg=$(TOPOLOGY_COVERPKG) \
		-coverprofile=coverage.topology.pilot-audit.out ./benchmarks/agent-durability/topology/matrix \
		-run TestAuditRejectedPilotEvidenceReconstructsEveryPair
	@set -e; for test in $(TOPOLOGY_COVER_INTEGRATIONS); do \
		go test -race -count=1 -coverpkg=$(TOPOLOGY_COVERPKG) \
			-coverprofile="coverage.topology.$$test.out" ./benchmarks/agent-durability/topology/semantics -run "$$test"; \
	done
	go run ./benchmarks/agent-durability/topology/cmd/covermerge \
		--output coverage.topology.out $(TOPOLOGY_COVER_BASE_PROFILES)
	go tool cover -func=coverage.topology.out
	@topology_coverage=$$(go tool cover -func=coverage.topology.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$topology_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "topology correctness coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

check-topology-controlled-host:
	@test "$${TOPOLOGY_CONTROLLED_HOST:-}" = "1" || (echo "TOPOLOGY_CONTROLLED_HOST=1 is required after provisioning the documented controlled host"; exit 1)
	@set -e; sample=1; while [ $$sample -le 3 ]; do \
		if ! load=$$(awk 'NR == 1 && NF == 5 { value = $$1 } END { if (NR != 1 || NF != 5 || value == "") exit 1; print value }' "$${TOPOLOGY_LOADAVG_PATH:-/proc/loadavg}"); then \
			echo 'controlled-host admission data is malformed: load average'; exit 1; \
		fi; \
		if ! pressure=$$(awk '$$1 == "some" { some++; for (i = 2; i <= NF; i++) { split($$i, field, "="); if (field[1] == "avg10") { avg10++; value = field[2] } } } END { if (some != 1 || avg10 != 1 || value == "") exit 1; print value }' "$${TOPOLOGY_CPU_PRESSURE_PATH:-/proc/pressure/cpu}"); then \
			echo 'controlled-host admission data is malformed: CPU pressure'; exit 1; \
		fi; \
		awk -v load="$$load" -v pressure="$$pressure" 'BEGIN { exit !(load ~ /^[0-9]+([.][0-9]+)?$$/ && pressure ~ /^[0-9]+([.][0-9]+)?$$/) }' || \
			{ printf 'controlled-host admission data is malformed: load=%s cpu_psi_avg10=%s\n' "$$load" "$$pressure"; exit 1; }; \
		awk -v load="$$load" -v pressure="$$pressure" 'BEGIN { exit !(load < 8 && pressure < 5) }' || \
			{ printf 'controlled-host admission failed: load=%s cpu_psi_avg10=%s\n' "$$load" "$$pressure"; exit 1; }; \
		printf 'controlled-host sample %d/3: load=%s cpu_psi_avg10=%s\n' "$$sample" "$$load" "$$pressure"; \
		[ $$sample -eq 3 ] || sleep 15; sample=$$((sample + 1)); \
	done

coverage-topology-timing: check-topology-controlled-host coverage-topology
	@set -e; for shape in $(TOPOLOGY_COVER_SHAPES); do \
		TOPOLOGY_PILOT_V5_REGRESSION=1 go test -race -count=1 -coverpkg=$(TOPOLOGY_COVERPKG) \
			-coverprofile="coverage.topology.v5.$$shape.out" ./benchmarks/agent-durability/topology/semantics \
			-run "TestTemporalExecutorPilotV5FailureShapesRecoverRepeatedly/$$shape"; \
	done
	go run ./benchmarks/agent-durability/topology/cmd/covermerge \
		--output coverage.topology.timing.out $(TOPOLOGY_COVER_BASE_PROFILES) $(TOPOLOGY_TIMING_PROFILES)
	go tool cover -func=coverage.topology.timing.out
	@topology_coverage=$$(go tool cover -func=coverage.topology.timing.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$topology_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "topology timing coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

coverage-system-adapters: check-postgres-service
	@test -n "$(TEMPORAL_CLI_PATH)" || (echo "TEMPORAL_CLI_PATH is required"; exit 1)
	TEMPORAL_CLI_PATH="$(TEMPORAL_CLI_PATH)" POSTGRES_DSN="service=$(POSTGRES_SERVICE)" \
	go test -race \
		-coverpkg=./benchmarks/agent-durability/v2/abalive,./benchmarks/agent-durability/v2/calibration,./benchmarks/agent-durability/v2/evidence,./benchmarks/agent-durability/v2/oracle,./benchmarks/agent-durability/v2/postgresadapter,./benchmarks/agent-durability/v2/protocol,./benchmarks/agent-durability/v2/systemplan,./benchmarks/agent-durability/v2/systemsuite,./benchmarks/agent-durability/v2/temporaladapter \
		-coverprofile=coverage.agent-durability-v2-system.out ./benchmarks/agent-durability/v2/...
	go tool cover -func=coverage.agent-durability-v2-system.out
	@system_coverage=$$(go tool cover -func=coverage.agent-durability-v2-system.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$system_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "agent durability v2 system coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

clean:
	rm -rf bin
	rm -rf .coverage.codex-direct.*
	rm -f coverage.out coverage.completion.out coverage.external-effects.out coverage.cancellation.out coverage.agent-durability.out coverage.agent-durability-v2.out coverage.agent-durability-v2-system.out coverage.coding-agent-conformance.out coverage.claude-direct.base.out coverage.claude-direct.audit.out coverage.claude-direct.out coverage.claude-direct-transport.out coverage.codex-direct.out coverage.codex-direct-transport.out coverage.temporal-native-adapter.out coverage.topology.default.out $(TOPOLOGY_COVER_BASE_PROFILES) $(TOPOLOGY_TIMING_PROFILES) coverage.topology.v5.out coverage.topology.out coverage.topology.timing.out
