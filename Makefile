GO      ?= go
BIN     := bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= relay:$(VERSION)

.PHONY: all build test test-short lint fmt vet run example docker docker-run clean

all: lint test build

## build: compile the relay binary into ./bin
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN)/relay ./cmd/relay

## test: run the full test suite with the race detector
test:
	$(GO) test -race -count=1 ./...

## test-short: run tests without the race detector (faster)
test-short:
	$(GO) test -count=1 ./...

## lint: gofmt check + go vet
lint: vet
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

## fmt: format all Go files
fmt:
	gofmt -w .

## run: run the API + worker pool against ./relay.db
run:
	$(GO) run ./cmd/relay serve --db relay.db --addr :8080 --concurrency 8

## example: run the embedding example
example:
	$(GO) run ./examples/worker --db relay.db

## docker: build the container image
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

## docker-run: run the image with a persistent volume for the database
docker-run:
	docker run --rm -p 8080:8080 -v relay-data:/data $(IMAGE)

clean:
	rm -rf $(BIN) *.db *.db-wal *.db-shm coverage.out
