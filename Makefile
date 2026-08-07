.PHONY: build test test-live coverage clean

build:
	mkdir -p bin
	go build -o bin/lab-worker ./cmd/worker
	go build -o bin/agent-simulator ./cmd/agent-simulator
	go build -o bin/worker-death-experiment ./experiments/worker-death/cmd/experiment
	go build -o bin/activity-completion-identity-experiment ./experiments/activity-completion-identity/cmd/experiment

test:
	go test -race ./...

test-live:
	go test -race -v -run TestLiveTemporal -timeout 4m \
		./experiments/worker-death/internal/lab \
		./experiments/activity-completion-identity/internal/lab

coverage:
	go test -race -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out
	@core_coverage=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$core_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "core coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'
	go test -race -coverprofile=coverage.completion.out ./experiments/activity-completion-identity/internal/lab
	go tool cover -func=coverage.completion.out
	@lab_coverage=$$(go tool cover -func=coverage.completion.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}'); \
	awk -v coverage="$$lab_coverage" 'BEGIN { if (coverage + 0 < 80) { printf "completion identity coverage %.1f%% is below 80%%\n", coverage; exit 1 } }'

clean:
	rm -rf bin
	rm -f coverage.out coverage.all.out coverage.core.out
