#!/usr/bin/env bash
# Compares the two mget drivers from opensearch-go#1236 with the same client,
# the same documents and the same random reads: coordinator, which sends each
# mget as one request to a node that fans it out across the shards, and direct,
# which splits it by shard in the client and asks each node only for the
# documents it holds. Each configuration loads the index once, then runs a
# discarded warm-up and RUNS measured read phases of each driver, alternating
# between them so a busy machine slows both alike. Output goes to results/.
#
# Set OSGO to a checkout of opensearch-go with the mget drivers.
set -euo pipefail

cd "$(dirname "$0")/.."
: "${OSGO:?set OSGO to an opensearch-go checkout with the mget drivers (#1236)}"
DOCS=${DOCS:-1000000}
RUNS=${RUNS:-5}
MGETS=${MGETS:-20000}       # mget calls per measured run
MGET_DOCS=${MGET_DOCS:-100} # documents per mget
READERS=${READERS:-16}      # goroutines calling MGet at once
OUT=results
mkdir -p "$OUT" bin
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

GOWORK=$(workspace "$OSGO") build -tags mget -o bin/bench-mget ./cmd/bench

# pair measures one configuration through both drivers on one index.
pair() { # $1 = label, rest = flags
  local label=$1
  shift
  say "start: $label"
  docker run --rm --network osbench -v "$PWD/bin:/opt/bench:ro" alpine:3.22 \
    /opt/bench/bench-mget -arm mget -mget-driver both -docs "$DOCS" -runs "$RUNS" -warmup 1 \
    -mgets "$MGETS" -mget-docs "$MGET_DOCS" -readers "$READERS" "$@" | tee "$OUT/$label.log"
  say "done:  $label"
}

# One node: every document is local either way, so this is the direct driver's
# own cost, splitting and merging in the client.
up docker/single-node.yml 1 10000
pair mget-single -addr http://os1:9200 -discover -shards 6

# Three nodes, six primaries with a replica each.
ALL=(-addr http://os1:9200,http://os2:9200,http://os3:9200 -discover -shards 6 -replicas 1)
up docker/cluster.yml 3 10000
pair mget-cluster "${ALL[@]}"

# Through an alias the client cannot route, so the direct driver sends every
# document through a coordinating node: it should match the coordinator.
pair mget-cluster-alias "${ALL[@]}" -alias

# One node held to one CPU. A coordinator waits for the slowest shard request
# of each mget; the direct driver waits for that node's documents only.
docker update --cpus 1 osbench-os3 >/dev/null
say "os3 held to 1 CPU"
pair mget-cluster-slow "${ALL[@]}"

docker compose -f docker/cluster.yml down -v >/dev/null 2>&1 || true
say "ALL DONE"
