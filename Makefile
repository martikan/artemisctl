# Variables
BINARY_NAME=artemisctl
MAIN_PATH=./cmd/artemisctl/main.go
BUILD_DIR=./bin
COVERAGE_FILE=coverage.out

# Linker flags to strip debug information
LDFLAGS=-ldflags="-s -w"

.PHONY: all build clean test coverage release help it-clean fixtures

# Name of the shared, reused integration-test broker container.
IT_BROKER=artemisctl-it-broker

# Default target when you just type 'make'
all: clean test build

## build: Compile the CLI for your current operating system
build:
	@echo "==> Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "==> Done. Binary is in $(BUILD_DIR)/$(BINARY_NAME)"

## test: Run all tests (including our Testcontainers broker tests)
test:
	@echo "==> Running linting..."
	gofmt -s -w .
	@echo "==> Running go vet..."
	go vet ./...
	@echo "==> Running tests..."
	go test -v -p 1 ./internal/...

## coverage: Run tests with coverage for CI
coverage-ci:
	@echo "==> Running tests with coverage..."
	go test -covermode=atomic -race -p 1 -coverprofile=$(COVERAGE_FILE) ./internal/...
	@echo "==> Done."

## coverage: Run tests with coverage and generate an HTML report
coverage: coverage-ci
	@echo "==> Generating HTML report..."
	go tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "==> Done. Open coverage.html in your browser."

## release: Cross-compile the CLI for Linux, macOS (Darwin), and Windows
release: clean
	@echo "==> Building release binaries..."
	@mkdir -p $(BUILD_DIR)/release

	GOOS=linux CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v3 go build $(LDFLAGS) -o $(BUILD_DIR)/release/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)

	GOOS=darwin CGO_ENABLED=0 GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/release/$(BINARY_NAME)-darwin-arm64 $(MAIN_PATH)

	@echo "==> Release binaries are in $(BUILD_DIR)/release/"

## clean: Remove build artifacts and coverage files
clean:
	@echo "==> Cleaning up..."
	@rm -rf $(BUILD_DIR)
	@echo "==> Cleaned."
	@echo "==> go mod tidy to clean up go.mod and go.sum..."
	@go mod tidy

## fixtures: Regenerate internal/journal/testdata from a live 2.42 container
# The harvester is an integration test gated behind ARTEMISCTL_HARVEST=1 (so
# plain `make test` skips it). It uses a dedicated container, not the shared
# reused broker, because it stops the broker to freeze the journal.
fixtures:
	@echo "==> Harvesting Artemis 2.42 data-dir fixture..."
	ARTEMISCTL_HARVEST=1 go test -run TestHarvestFixture -v -p 1 -timeout 15m ./internal/journal/
	@echo "==> Done. See internal/journal/testdata/"

## it-clean: Remove the shared integration-test broker container
# Integration tests reuse one broker (Reuse: true) and never terminate it, and
# Ryuk cannot reap reused containers (and is disabled under rootless podman), so
# the broker lingers on purpose. Run this to force-remove it.
it-clean:
	@echo "==> Removing shared integration broker $(IT_BROKER)..."
	@docker rm -f $(IT_BROKER) 2>/dev/null || true
	@echo "==> Done."

## help: Show this help message
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build    - Compile the CLI for your current operating system"
	@echo "  test     - Run all tests"
	@echo "  coverage - Run tests with coverage and generate an HTML report"
	@echo "  release  - Cross-compile the CLI for Linux, macOS, and Windows"
	@echo "  fixtures - Regenerate internal/journal/testdata from a live 2.42 container"
	@echo "  it-clean - Remove the shared integration-test broker container"
	@echo "  clean    - Remove build artifacts"
