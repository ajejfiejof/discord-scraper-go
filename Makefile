BINARY := scraper
MODULE := github.com/pratherbytecraft/discord-scraper-go
GO := go

.PHONY: all build vet lint test run clean install

all: vet build

build:
	$(GO) build -o bin/$(BINARY) ./cmd/scraper
	@echo "built bin/$(BINARY)"

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...
	gofmt -w -s .

lint:
	@which golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed, skipping (go vet already ran)"

test:
	$(GO) test ./... -count=1 -race -cover

run: build
	./bin/$(BINARY) --help

clean:
	rm -rf bin/ output/ dist/

install: build
	install -m 755 bin/$(BINARY) $(GOPATH)/bin/$(BINARY) 2>/dev/null || install -m 755 bin/$(BINARY) /usr/local/bin/$(BINARY)

# quick check before commit
check: vet test
	@echo "all checks passed"

tidy:
	$(GO) mod tidy
