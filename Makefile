.DEFAULT_GOAL := dev

.PHONY: dev
dev: ## dev build
dev: clean install generate build fmt lint test mod-tidy build-snapshot 

.PHONY: dev-no-lint
dev-no-lint: ## dev build (no lint)
dev: clean install generate build fmt test mod-tidy build-snapshot 

.PHONY: ci
ci: ## CI build
ci: dev diff

.PHONY: clean
clean: ## remove files created during build
	$(call print-target)
	rm -rf dist
	rm -f coverage.*

.PHONY: install
install: ## go install tools
	$(call print-target)
	cd tools && go install $(shell cd tools && go list -f '{{ join .Imports " " }}' -tags=tools)

.PHONY: generate
generate: ## go generate
	$(call print-target)
	go generate ./...

.PHONY: gen-proto
gen-proto: ## generate protobuf
	$(call print-target)
	docker run --rm -u 1000 -v ${PWD}:${PWD} -w ${PWD} \
		xumr0x/protobuf-nrpc:latest \
		--proto_path=${PWD}/internal/proto \
		--go_out=${PWD}/internal/proto \
		-I/usr/include/github.com/nats-rpc/nrpc \
		${PWD}/internal/proto/microdb.proto

.PHONY: build
build: ## go build
	$(call print-target)
	CGO_ENABLED=0 GOOS=linux go build -ldflags "-extldflags -static" \
		-o ./bin/microdb-publisher ./cmd/publisher/*.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags "-extldflags -static" \
		-o ./bin/microdb-querier ./cmd/querier/*.go

.PHONY: build-docker
build-docker: ## docker build
	$(call print-target)
	docker build -t microdb/querier:latest -f docker/releases/Dockerfile.querier .
	docker build -t microdb/publisher:latest -f docker/releases/Dockerfile.publisher .

.PHONY: fmt
fmt: ## go fmt
	$(call print-target)
	go fmt ./...

.PHONY: lint
lint: ## golangci-lint
	$(call print-target)
	golangci-lint run

.PHONY: test
test: ## go test with race detector and code covarage
	$(call print-target)
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

.PHONY: mod-tidy
mod-tidy: ## go mod tidy
	$(call print-target)
	go mod tidy
	cd tools && go mod tidy

.PHONY: build-snapshot
build-snapshot: ## goreleaser --snapshot --skip-publish --rm-dist
	$(call print-target)
	goreleaser --snapshot --skip-publish --rm-dist

.PHONY: diff
diff: ## git diff
	$(call print-target)
	git diff --exit-code
	RES=$$(git status --porcelain) ; if [ -n "$$RES" ]; then echo $$RES && exit 1 ; fi

.PHONY: release
release: ## goreleaser --rm-dist
release: install
	$(call print-target)
	goreleaser --rm-dist

.PHONY: run
run: ## go run
	@go run -race ./cmd/publisher

.PHONY: go-clean
go-clean: ## go clean build, test and modules caches
	$(call print-target)
	go clean -r -i -cache -testcache -modcache

.PHONY: docker
docker: ## run in golang container, example: make docker run="make ci"
	docker run --rm \
		-v $(CURDIR):/repo $(args) \
		-w /repo \
		golang:1.16 $(run)

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

define print-target
    @printf "Executing target: \033[36m$@\033[0m\n"
endef
