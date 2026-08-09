.PHONY: build test test-live check-postgres-service check-publication-v2-config publication-v2 test-system-adapters test-temporal-native coverage coverage-system-adapters evidence-temporal-native evidence-claude-direct clean

PUBLICATION_DEADLINE ?= 2h

build:
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
	go build -o bin/temporal-native-evidence ./experiments/durable-vendor-sessions/temporal-native/cmd/temporal-native-evidence
	go build -o bin/claude-direct-worker ./experiments/durable-vendor-sessions/claude-direct/cmd/worker
	go build -o bin/claude-direct-effect ./experiments/durable-vendor-sessions/claude-direct/cmd/controlled-effect
	go build -o bin/claude-direct-launcher ./experiments/durable-vendor-sessions/claude-direct/cmd/claude-launcher
	go build -o bin/claude-direct-experiment ./experiments/durable-vendor-sessions/claude-direct/cmd/experiment

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
		--max-turns 2 \
		--trials 3

coverage:
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
	go test -race -coverpkg=./benchmarks/agent-durability/calibration,./benchmarks/agent-durability/evidence,./benchmarks/agent-durability/livecommon,./benchmarks/agent-durability/oracle,./benchmarks/agent-durability/protocol -coverprofile=coverage.agent-durability.out ./benchmarks/agent-durability/...
	go tool cover -func=coverage.agent-durability.out
	@harness_coverage=$$(go tool cover -func=coverage.agent-durability.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$harness_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "agent durability harness coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.claude-direct.out ./experiments/durable-vendor-sessions/claude-direct/internal/lab
	go tool cover -func=coverage.claude-direct.out
	@lab_coverage=$$(go tool cover -func=coverage.claude-direct.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "Claude direct lab coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
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
	rm -f coverage.out coverage.completion.out coverage.external-effects.out coverage.cancellation.out coverage.agent-durability.out coverage.agent-durability-v2.out coverage.agent-durability-v2-system.out coverage.claude-direct.out coverage.temporal-native-adapter.out
