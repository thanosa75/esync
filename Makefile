# esync — build, test, and documentation targets.
# `make` or `make help` lists everything. Variables below are override-friendly:
#   make build MAIN_PKG=. VERSION=1.2.3
#   make diagrams FORMAT=png
#   make dist

SHELL      := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# --- Layout ----------------------------------------------------------------
# ARCHITECTURE.md §17 places the entrypoint at the module root.
MAIN_PKG   ?= .
BIN_DIR    ?= bin
BUILD_DIR  ?= build
DIST_DIR   ?= dist
BIN        ?= $(BIN_DIR)/esync

DOC_DIR      ?= doc
DIAGRAM_SRC  ?= $(DOC_DIR)/ARCHITECTURE.md
DIAGRAM_DIR  ?= $(DOC_DIR)/images

# --- Version stamping (best-effort; degrades to "dev" without git) ---------
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS ?= -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

# CGO off keeps the binary static and dependency-free (ARCHITECTURE.md I-BUILD-01).
GO             ?= go
GO_BUILD_FLAGS ?= -trimpath
GOFLAGS_ENV    ?= CGO_ENABLED=0

# --- Test / coverage knobs -----------------------------------------------------
COVER_OUT  ?= $(BUILD_DIR)/coverage.out
COVER_HTML ?= $(BUILD_DIR)/coverage.html
COVER_MIN  ?= 80
# Attribute coverage from every test (incl. the root end-to-end test) to every
# package — the transfer engine is exercised through sender.Run / receiver.Run.
COVERPKG   ?= ./...
FUZZTIME   ?= 30s
PKGS       ?= ./...

# --- Cross-compile matrix (ARCHITECTURE.md REQ-NFR-020) -----------------------
PLATFORMS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

# --- mermaid-cli (https://github.com/mermaid-js/mermaid-cli) ------------------
MMDC       ?= mmdc
FORMAT     ?= svg
MMDC_FLAGS ?= --theme neutral --backgroundColor transparent

# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@awk 'BEGIN{FS=":.*##"; printf "esync — make targets\n\n"} \
		/^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: tidy fmt vet build test ## Tidy, format, vet, build, and test

# --- Build -----------------------------------------------------------------
.PHONY: build
build: ## Build the esync binary into ./bin
	@mkdir -p $(BIN_DIR)
	$(GOFLAGS_ENV) $(GO) build $(GO_BUILD_FLAGS) -ldflags '$(LDFLAGS)' -o $(BIN) $(MAIN_PKG)
	@echo "built $(BIN) ($(VERSION))"

.PHONY: build-all
build-all: ## Compile every package (no binary output)
	$(GO) build $(GO_BUILD_FLAGS) $(PKGS)

.PHONY: install
install: ## Install esync into $GOBIN / $GOPATH/bin
	$(GOFLAGS_ENV) $(GO) install $(GO_BUILD_FLAGS) -ldflags '$(LDFLAGS)' $(MAIN_PKG)

.PHONY: run
run: ## Run esync (pass args via ARGS="...")
	$(GO) run $(MAIN_PKG) $(ARGS)

.PHONY: dist
dist: ## Cross-compile static binaries for all supported platforms
	@mkdir -p $(DIST_DIR)
	@for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		out=$(DIST_DIR)/esync-$(VERSION)-$$os-$$arch; \
		[ "$$os" = windows ] && out=$$out.exe || true; \
		echo "  build $$out"; \
		$(GOFLAGS_ENV) GOOS=$$os GOARCH=$$arch \
			$(GO) build $(GO_BUILD_FLAGS) -ldflags '$(LDFLAGS)' -o $$out $(MAIN_PKG); \
	done
	@cd $(DIST_DIR) && sha256sum esync-$(VERSION)-* > SHA256SUMS && echo "  wrote $(DIST_DIR)/SHA256SUMS"

# --- Formatting / static analysis ----------------------------------------------
.PHONY: fmt
fmt: ## Format all Go source with gofmt
	gofmt -l -w .

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "not gofmt-clean:"; echo "$$unformatted"; exit 1; fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKGS)

.PHONY: lint
lint: ## Run golangci-lint if installed
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run $(PKGS); \
	else \
		echo "golangci-lint not installed — run 'make tools'"; \
	fi

.PHONY: check-goroutines
check-goroutines: ## Heuristic gate: no bare `go` statement outside obs.Go (REQ-ERR-006)
	@files=$$(find . -name '*.go' -not -name '*_test.go' \
		-not -path './internal/obs/*' -not -path './vendor/*' 2>/dev/null); \
	if [ -n "$$files" ] && grep -nE '^[[:space:]]*go[[:space:]]+[A-Za-z_]' $$files | grep -v 'obs\.Go('; then \
		echo "bare 'go' statement found — start goroutines via obs.Go (ARCHITECTURE.md §14.5)"; exit 1; \
	fi; \
	echo "no bare goroutines"

# --- Modules --------------------------------------------------------------
.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod / go.sum are not tidy
	$(GO) mod tidy -diff

.PHONY: generate
generate: ## Run go generate
	$(GO) generate $(PKGS)

# --- Tests ---------------------------------------------------------------------
.PHONY: test
test: ## Run the full test suite with the race detector
	$(GO) test -race $(PKGS)

.PHONY: test-short
test-short: ## Run only fast tests (-short)
	$(GO) test -short $(PKGS)

.PHONY: cover
cover: ## Run tests with coverage and print the total
	@mkdir -p $(BUILD_DIR)
	$(GO) test -race -covermode=atomic -coverpkg=$(COVERPKG) -coverprofile=$(COVER_OUT) $(PKGS)
	@$(GO) tool cover -func=$(COVER_OUT) | tail -1

.PHONY: cover-html
cover-html: cover ## Open an HTML coverage report
	$(GO) tool cover -html=$(COVER_OUT) -o $(COVER_HTML)
	@echo "wrote $(COVER_HTML)"

.PHONY: cover-check
cover-check: ## Fail if total coverage is below COVER_MIN (default 80)
	@mkdir -p $(BUILD_DIR)
	@$(GO) test -race -covermode=atomic -coverpkg=$(COVERPKG) -coverprofile=$(COVER_OUT) $(PKGS) >/dev/null
	@pct=$$($(GO) tool cover -func=$(COVER_OUT) | awk '/^total:/{gsub(/%/,"",$$3); print $$3}'); \
	pct=$${pct:-0}; \
	echo "total coverage: $$pct% (min $(COVER_MIN)%)"; \
	awk "BEGIN{exit !($$pct+0 >= $(COVER_MIN))}" || { echo "coverage below $(COVER_MIN)%"; exit 1; }

.PHONY: bench
bench: ## Run benchmarks
	$(GO) test -run '^$$' -bench . -benchmem $(PKGS)

.PHONY: fuzz
fuzz: ## Run each Go fuzz target briefly (override with FUZZTIME=...)
	@targets=$$(grep -rlE 'func Fuzz[A-Za-z0-9_]+\(' --include='*_test.go' . || true); \
	if [ -z "$$targets" ]; then echo "no fuzz targets found"; exit 0; fi; \
	for d in $$(echo "$$targets" | xargs -n1 dirname | sort -u); do \
		for fn in $$(grep -hoE 'func Fuzz[A-Za-z0-9_]+' $$d/*_test.go | awk '{print $$2}' | sort -u); do \
			echo "  fuzz $$d $$fn"; \
			$(GO) test -run '^$$' -fuzz "^$$fn$$" -fuzztime=$(FUZZTIME) $$d; \
		done; \
	done

# --- Documentation / diagrams ------------------------------------------------
.PHONY: docs
docs: diagrams ## Build all generated documentation

.PHONY: images
images: diagrams ## Alias for `diagrams`

.PHONY: diagrams
diagrams: ## Render the mermaid diagrams in doc/ARCHITECTURE.md to doc/images/
	@command -v $(MMDC) >/dev/null 2>&1 || { \
		echo "$(MMDC) not found — install with: npm install -g @mermaid-js/mermaid-cli"; exit 1; }
	@mkdir -p $(DIAGRAM_DIR)
	@rm -f $(DIAGRAM_DIR)/*.mmd
	@awk ' \
		/^#+ /{ h=$$0; sub(/^#+[ ]+/,"",h); sub(/^[0-9.]+[ ]+/,"",h); \
			gsub(/[^A-Za-z0-9]+/,"-",h); h=tolower(h); gsub(/^-|-$$/,"",h); next } \
		/^```mermaid[ ]*$$/ { n++; name=(h==""?"diagram-" n:h); \
			f="$(DIAGRAM_DIR)/" name ".mmd"; printf "" > f; inblk=1; next } \
		inblk && /^```[ ]*$$/ { inblk=0; close(f); next } \
		inblk { print >> f } \
	' $(DIAGRAM_SRC)
	@for m in $(DIAGRAM_DIR)/*.mmd; do \
		out=$${m%.mmd}.$(FORMAT); \
		echo "  render $$out"; \
		$(MMDC) --input "$$m" --output "$$out" $(MMDC_FLAGS); \
	done
	@rm -f $(DIAGRAM_DIR)/*.mmd
	@echo "diagrams written to $(DIAGRAM_DIR)/"

# --- Aggregate checks ------------------------------------------------------
.PHONY: verify
verify: tidy-check fmt-check vet lint check-goroutines test ## Run every pre-commit check

.PHONY: ci
ci: tidy-check fmt-check vet check-goroutines cover-check ## Gates enforced in CI (ARCHITECTURE.md §19.8)

# --- Tooling / cleanup ---------------------------------------------------------
.PHONY: tools
tools: ## Install development tooling
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "mermaid-cli: npm install -g @mermaid-js/mermaid-cli"

.PHONY: clean
clean: ## Remove build, dist, and coverage artifacts
	$(GO) clean
	rm -rf $(BIN_DIR) $(BUILD_DIR) $(DIST_DIR)

.PHONY: clean-diagrams
clean-diagrams: ## Remove rendered diagrams
	rm -rf $(DIAGRAM_DIR)
