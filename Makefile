.PHONY: build test vet lint bench bench-bulkindexer suite cluster cluster-stop stress stress-stop clean

DOCS ?= 1000000
RUNS ?= 5
SHARDS ?= 1
WORKERS ?= 8
ADDR ?= http://localhost:9200

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

# One node, one shard: the talk's shape.
bench:
	go run ./cmd/bench -arm handrolled -addr $(ADDR) -docs $(DOCS) -runs $(RUNS) -shards $(SHARDS) -workers $(WORKERS)

bench-bulkindexer:
	go run ./cmd/bench -arm bulkindexer -addr $(ADDR) -docs $(DOCS) -runs $(RUNS) -shards $(SHARDS) -workers $(WORKERS)

# Every configuration, loader in a container on the cluster network. Set OSGO to
# an opensearch-go checkout to compare that client too.
suite:
	scripts/suite.sh

cluster:
	docker compose -f docker/cluster.yml up -d

cluster-stop:
	docker compose -f docker/cluster.yml down -v

# Single node with a two slot write queue: the cluster rejects constantly.
stress:
	WRITE_QUEUE_SIZE=2 docker compose -f docker/single-node.yml up -d

stress-stop:
	docker compose -f docker/single-node.yml down -v

clean:
	rm -rf bench bin
