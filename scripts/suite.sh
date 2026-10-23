#!/usr/bin/env bash
# Runs the full comparison: the loaders on one node, under a two slot write
# queue, and on a three node cluster with 24 shards. Each configuration gets a
# discarded warm-up and RUNS measured runs. Output goes to results/.
#
# The loader runs in a container on the cluster's network, so it reaches every
# node at the address the node publishes (node discovery works) and no host
# port forwarder sits in the path.
#
# Set OSGO to a checkout of opensearch-go to also run that client: the same
# bulkindexer arm, and the bulkretry arm, which needs the BulkIndexer from
# opensearch-go#1219.
set -euo pipefail

cd "$(dirname "$0")/.."
DOCS=${DOCS:-1000000}
RUNS=${RUNS:-5}
PRODUCERS=${PRODUCERS:-16} # goroutines calling Add; the hand-rolled arm ignores it
OUT=results
mkdir -p "$OUT" bin
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

rm -f bin/bench-pr
build -o bin/bench ./cmd/bench
if [ -n "${OSGO:-}" ]; then
  GOWORK=$(workspace "$OSGO") build -tags bulkretry -o bin/bench-pr ./cmd/bench
fi

bench() { # $1 = label, $2 = binary, rest = flags; ENV adds -e flags
  local label=$1 bin=$2
  shift 2
  [ -x "bin/$bin" ] || return 0 # no OSGO, no second client
  say "start: $label"
  # shellcheck disable=SC2086 # ENV holds whole -e flags
  docker run --rm --network osbench ${ENV:-} -v "$PWD/bin:/opt/bench:ro" alpine:3.22 \
    "/opt/bench/$bin" -docs "$DOCS" -runs "$RUNS" -warmup 1 -producers "$PRODUCERS" "$@" | tee "$OUT/$label.log"
  say "done:  $label"
}

ONE=(-addr http://os1:9200)
up docker/single-node.yml 1 10000
bench single-handrolled     bench    "${ONE[@]}" -arm handrolled
bench single-bulkindexer    bench    "${ONE[@]}" -arm bulkindexer
bench single-bulkindexer-pr bench-pr "${ONE[@]}" -arm bulkindexer
bench single-bulkretry-pr   bench-pr "${ONE[@]}" -arm bulkretry

up docker/single-node.yml 1 2
bench stress-handrolled-window   bench    "${ONE[@]}" -arm handrolled  -workers 32 -batch 500
bench stress-handrolled-attempt3 bench    "${ONE[@]}" -arm handrolled  -workers 32 -batch 500 -max-attempts 3
bench stress-bulkindexer-window  bench    "${ONE[@]}" -arm bulkindexer -workers 32 -flush-bytes 100000
bench stress-bulkretry-window-pr bench-pr "${ONE[@]}" -arm bulkretry   -workers 32 -flush-bytes 100000

# Every arm discovers the nodes, so the only difference left is the indexer.
ALL=(-addr http://os1:9200,http://os2:9200,http://os3:9200 -discover -shards 24)
up docker/cluster.yml 3 10000
bench cluster24-handrolled          bench    "${ALL[@]}" -arm handrolled
bench cluster24-bulkindexer         bench    "${ALL[@]}" -arm bulkindexer
bench cluster24-bulkindexer-pr      bench-pr "${ALL[@]}" -arm bulkindexer
bench cluster24-bulkretry-grouped   bench-pr "${ALL[@]}" -arm bulkretry -warm-shard-map
bench cluster24-bulkretry-ungrouped bench-pr "${ALL[@]}" -arm bulkretry -no-shard-router
FIXED="-e OPENSEARCH_GO_BULK_SHARD=fixed -e OPENSEARCH_GO_BULK_NODE=fixed"
ENV=$FIXED bench cluster24-bulkretry-grouped-fixed   bench-pr "${ALL[@]}" -arm bulkretry -warm-shard-map
ENV=$FIXED bench cluster24-bulkretry-ungrouped-fixed bench-pr "${ALL[@]}" -arm bulkretry -no-shard-router

# An eight slot write queue per node: here the number of write tasks per
# request, which shard grouping cuts, decides who gets rejected.
up docker/cluster.yml 3 8
bench cluster24q8-handrolled          bench    "${ALL[@]}" -arm handrolled
bench cluster24q8-bulkindexer         bench    "${ALL[@]}" -arm bulkindexer
bench cluster24q8-bulkretry-grouped   bench-pr "${ALL[@]}" -arm bulkretry -warm-shard-map
bench cluster24q8-bulkretry-ungrouped bench-pr "${ALL[@]}" -arm bulkretry -no-shard-router

docker compose -f docker/cluster.yml down -v >/dev/null 2>&1 || true
say "ALL DONE"
