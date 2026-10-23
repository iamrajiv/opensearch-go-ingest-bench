# opensearch-go-ingest-bench

> A benchmark harness for OpenSearch ingestion in Go: how fast a loader goes, how much work the cluster has to do, and whether anything was lost on the way. It also reads the documents back with `mget`, to compare the client's two mget drivers.

[![CI](https://github.com/iamrajiv/opensearch-go-ingest-bench/actions/workflows/ci.yml/badge.svg)](https://github.com/iamrajiv/opensearch-go-ingest-bench/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/iamrajiv/opensearch-go-ingest-bench)](https://goreportcard.com/report/github.com/iamrajiv/opensearch-go-ingest-bench)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?logo=go)](go.mod)

## The problem

Ingestion numbers are usually reported as one figure from one run, with no
statement about whether every document arrived. Both halves matter. OpenSearch
answers `HTTP 200` for a bulk request where individual documents were rejected,
most often with `429 rejected_execution_exception` when the write queue is full,
so a loader can look fast and healthy while quietly dropping data.

This harness measures both, for more than one loader, under the same conditions.

## What it measures

Each run creates a fresh index, loads it, then asks the index how many documents
it actually holds.

| Reported | Why it is there |
|---|---|
| docs/s, median and full spread | a single run on a laptop means little |
| bulk requests sent | the client's side of the work |
| **write tasks** created | the cluster's side: one bulk request becomes one task per shard it touches |
| 429s the cluster handed out | how hard you were pushing |
| documents retried | what the retry policy had to do |
| documents dropped | what a careless loader would have lost |
| **lossless runs** | documents in versus `_count` out, per run |

## Loaders

| Arm | What it is |
|---|---|
| `handrolled` | generate, batch, fixed worker pool, retry only the rejected documents with backoff and jitter, give up on a time window |
| `bulkindexer` | the same load through `opensearchutil.BulkIndexer`, which batches and runs the pool for you, with the retry policy still written by the caller |
| `bulkretry` | one long-lived `BulkIndexer` that retries by itself through `RetryPolicy` and groups documents by shard. Needs the indexer [opensearch-go#1219](https://github.com/opensearch-project/opensearch-go/pull/1219) shipped in v5.1.0; built with `-tags bulkretry` |

All three share the same give-up rule and backoff, so only the client strategy
differs. The `bulkindexer` arm is the same code for both clients: v5.1 keeps
v5.0's `OnFailure` as a deprecated alias of its new `OnError`. `go.mod` stays
on v5.0.3, the baseline, so the arms that need v5.1 or later build against a
checkout through a temporary `go.work` (`OSGO`, below).

## Reads

The `mget` arm loads the index once with the hand-rolled loader, checks it
holds every document, then measures read phases against it: `-mgets` calls of
`-mget-docs` random documents each, from `-readers` goroutines at once. It
compares the two drivers [opensearch-go#1236](https://github.com/opensearch-project/opensearch-go/pull/1236)
gives `DocumentClient.MGet`, with the same client, the same documents and the
same reads, so only the driver differs:

| Driver | What it does |
|---|---|
| `coordinator` | sends each mget as one request. The node that receives it splits the documents by shard, asks each shard's node for its share, and waits for the slowest before it answers |
| `direct` | splits each mget by shard in the client, from the shard map the transport keeps, and asks each node only for the documents it holds, at once |

Built with `-tags mget` against a checkout of opensearch-go main at `15c8e47`
or later, where #1236 merged; no release has it yet. `-mget-driver both`, which
the suite uses, measures both drivers on one index, alternating between them
run by run (coordinator then direct, then direct then coordinator), so a laptop
that gets busier during the runs slows both alike.

| Reported | Why it is there |
|---|---|
| docs/s, median and full spread | documents read, not mget calls |
| p50, p99 | how long one mget call takes, as the caller waits for it |
| requests per mget | the client's side: what the direct driver fans an mget out to |
| sent whole | mgets that went to a coordinating node unsplit: all of them for `coordinator`, and for `direct` the ones whose documents it could not route, such as through an alias |
| nodes per mget, retries, via coordinator | from the client's `MGetObserver`, for the mgets the direct driver splits |
| **get tasks** per mget | the cluster's side: one get thread pool task per shard an mget touches, on whichever node does the work |
| get rejections | how hard the reads pushed |
| **correct runs** | every document found, in the place it was asked for, from the index it is in |

The observer is only told about mgets the direct driver splits. One it hands to
the cluster whole, because no document routes to a node, is not reported, so the
harness counts those itself as mgets sent and not reported.

```
                           handrolled                              bulkindexer, bulkretry
generate -> [chan Event] -> batcher -> [chan []Event] -> workers    generate -> BulkIndexer
                                                           |                        |
                                                           v                        v
                                                        _bulk                     _bulk
```

## Usage

```shell
# every configuration below, 5 runs each plus a warm-up, logs in results/
scripts/suite.sh

# the same, also running a checkout of opensearch-go at v5.1 or later
OSGO=../opensearch-go scripts/suite.sh

# the mget drivers, against a checkout of opensearch-go main with #1236: one
# node, three nodes, three nodes through an alias, and three nodes with one held
# to one CPU
OSGO=../opensearch-go scripts/mget-suite.sh
```

The indexer arms call `Add` from 16 goroutines by default (`PRODUCERS=1` for
one), the way a service shares one indexer between its handlers. The suite
also runs the v5.1 indexer with both sizing controllers fixed
(`OPENSEARCH_GO_BULK_SHARD=fixed`, and `OPENSEARCH_GO_BULK_TARGET=fixed`, which
was `OPENSEARCH_GO_BULK_NODE` before
[opensearch-go#1231](https://github.com/opensearch-project/opensearch-go/pull/1231);
the suite sets both).

The suite runs the loader in a container on the cluster's Docker network. That
way the client can reach every node at the address the node publishes, which
node discovery and shard grouping need. From the host, a discovering client is
handed those internal addresses and cannot connect, so `-discover` is off by
default for runs by hand.

One configuration by hand:

```shell
docker compose -f docker/single-node.yml up -d
go run ./cmd/bench -arm handrolled  -docs 1000000 -runs 5
go run ./cmd/bench -arm bulkindexer -docs 1000000 -runs 5

# a two slot write queue, so the cluster rejects constantly
WRITE_QUEUE_SIZE=2 docker compose -f docker/single-node.yml up -d
go run ./cmd/bench -arm handrolled -workers 32 -batch 500 -runs 5
go run ./cmd/bench -arm handrolled -workers 32 -batch 500 -max-attempts 3 -runs 5
```

The direct mget driver needs node discovery, so the mget arm runs from inside
the cluster's network too. `scripts/mget-suite.sh` builds it as `bin/bench-mget`,
which then runs one configuration by hand:

```shell
docker run --rm --network osbench -v "$PWD/bin:/opt/bench:ro" alpine:3.22 \
  /opt/bench/bench-mget -arm mget -addr http://os1:9200 -discover -shards 6 -mget-driver direct
```

Flags: `-addr -user -pass -index -arm -discover -request-timeout -docs -runs
-warmup -shards -replicas -refresh -workers -batch -queue -flush-bytes
-flush-interval -producers -retry-for -max-attempts -no-shard-router
-warm-shard-map`, and for the mget arm `-mgets -mget-docs -readers
-mget-driver -alias`.

## Method

- one warm-up run per configuration, discarded, because caches start cold;
- every configuration run several times, reported as the **median with the full
  spread**, never a single number;
- a fresh index per run, created with `number_of_replicas: 0` and
  `refresh_interval: -1`, restored and refreshed before counting. With
  `-replicas`, the write tasks include the replica copies, since replicas
  index on the write pool too;
- the document count comes from the **index**, not from the loader's own
  counters, so a run proves what it stored;
- write tasks and rejections come from the delta of the write thread pool's
  `completed` and `rejected` counters across every node;
- every request attempt times out after `-request-timeout`, so a stalled
  connection becomes a resend and not a hung run;
- for reads, the index is loaded once per configuration and every read phase
  asks for the same random documents through either driver. Every answer is
  checked against the document asked for, and with replicas the reads wait
  for them to start.

A laptop is not a measuring instrument. Read the ratios between rows, and treat
the lossless column as the result that should never move.

## Results

19 October 2026. A MacBook (Apple silicon) running a Colima VM with 8 CPUs and
16 GB, OpenSearch 3.8.0 with a 2 GB heap per node, the loader in a container on
the same VM, Go 1.27.2, opensearch-go v5.0.3 and the #1219 branch at `80d55de`.
1,000,000 documents per run, median of 5 runs after a discarded warm-up, one
goroutine calling `Add`. "Rejected" is write tasks the cluster refused;
"retried" is documents sent again.

The suite was run again on `2b10d68` and on `89b482b` (16 goroutines calling
`Add`). The laptop was busier both times, so every row is slower, v5.0.3 with
the same code included, and the cluster runs on `89b482b` were too noisy to
quote. The single node and stress rows kept the same shape: the #1219 indexer
43 to 45% faster than v5.0.3 on one node, and under a two slot queue never
rejected but about half the speed of v5.0.3 with caller retries.

**One node, one shard.**

| Loader | Client | docs/s | Spread | Requests | Write tasks | Lossless |
|---|---|--:|--:|--:|--:|:-:|
| handrolled | v5.0.3 | 202,659 | 199,387 - 209,181 | 505 | 513 | 5/5 |
| bulkindexer | v5.0.3 | 53,587 | 52,615 - 54,850 | 40 | 41 | 5/5 |
| bulkindexer | #1219 | 87,974 | 84,891 - 90,102 | 71 | 72 | 5/5 |
| bulkretry | #1219 | 86,773 | 85,987 - 88,330 | 72 | 73 | 5/5 |

**One node, two slot write queue**, 32 workers, 100 KB batches.

| Loader | Client | docs/s | Spread | Rejected | Retried | Dropped | Lossless |
|---|---|--:|--:|--:|--:|--:|:-:|
| handrolled, 2 minute window | v5.0.3 | 171,935 | 114,692 - 175,600 | 447 | 223,698 | 0 | 5/5 |
| handrolled, 3 attempts | v5.0.3 | 189,501 | 179,891 - 195,341 | 470 | 225,460 | 9,100 | **0/5** |
| bulkindexer, caller retries | v5.0.3 | 98,662 | 89,115 - 102,179 | 70 | 36,780 | 0 | 5/5 |
| bulkretry | #1219 | 57,290 | 55,382 - 58,587 | 0 | 0 | 0 | 5/5 |

**Three nodes, 24 shards.** Every arm discovers the nodes.

| Loader | Client | docs/s | Spread | Requests | Write tasks | Lossless |
|---|---|--:|--:|--:|--:|:-:|
| handrolled | v5.0.3 | 182,595 | 148,491 - 190,276 | 507 | 12,098 | 5/5 |
| bulkindexer | v5.0.3 | 120,828 | 114,496 - 128,002 | 40 | 984 | 5/5 |
| bulkindexer | #1219 | 150,038 | 135,843 - 180,145 | 91 | 99 | 5/5 |
| bulkretry, grouped | #1219 | 135,149 | 115,593 - 152,990 | 78 | 83 | 5/5 |
| bulkretry, ungrouped | #1219 | 203,541 | 182,551 - 221,274 | 50 | 1,253 | 5/5 |

**Three nodes, 24 shards, eight slot write queue.**

| Loader | Client | docs/s | Spread | Write tasks | Rejected | Retried | Lossless |
|---|---|--:|--:|--:|--:|--:|:-:|
| handrolled | v5.0.3 | 122,043 | 110,291 - 130,791 | 12,120 | 1,010 | 84,249 | 5/5 |
| bulkindexer | v5.0.3 | 100,903 | 98,143 - 110,730 | 984 | 0 | 0 | 5/5 |
| bulkretry, grouped | #1219 | 101,436 | 93,140 - 136,864 | 80 | 0 | 0 | 5/5 |
| bulkretry, ungrouped | #1219 | 92,493 | 71,410 - 119,343 | 2,452 | 332 | 108,145 | 5/5 |

What the numbers say:

- Nothing was lost in any configuration except the one that gives up after
  three attempts, which lost about 9,100 documents a run under the same load the
  time window survived.
- The #1219 indexer is a drop-in upgrade: the `bulkindexer` arm compiled
  unchanged and got faster, by 64% on one node and 24% on 24 shards, with a
  tenth of the write tasks.
- Grouping by shard cuts write tasks about fifteen times. Where the write queue
  is tight it is the difference between 332 rejections a run and none.
- The #1219 indexer keeps no more requests in flight to a node than the node
  has write threads, and one while it rejects. It is never rejected, but with
  nothing waiting in the queue the threads idle through each round trip, so it
  trails the hand-rolled loader when the cluster has room, and trails it most
  under the two slot queue.

All three nodes share one VM's CPUs here, so the write task count is the number
that carries over to a real cluster, more than docs/s does.

## Layout

```
cmd/bench              the runner: flags, repeated runs, the summary table
internal/bench         loaders, the mget arm, cluster setup, retry policy, statistics
docker                 single node, three node cluster, write queue size
scripts/suite.sh       every load configuration, loader in a container
scripts/mget-suite.sh  the mget drivers, side by side
scripts/lib.sh         what the two suites share
```

## Background

The hand-rolled loader is the design from
[Building a High Throughput OpenSearch Ingestor using Go](https://www.youtube.com/watch?v=gYamMwE_Zwk),
OpenSearchCon North America 2026
([slides and code](https://github.com/iamrajiv/opensearchcon-north-america-2026)).
This repository is where that measurement rig lives on, pointed at more than one
client strategy.

## License

[MIT](LICENSE)
