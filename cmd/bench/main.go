// Command bench loads documents into OpenSearch with one of several strategies
// and reports throughput, the work the cluster had to do, and whether anything
// was lost. The mget arm reads them back instead and checks every answer. Every
// configuration is run several times and reported as a median with its full
// spread, because a laptop is not a measuring instrument.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/iamrajiv/opensearch-go-ingest-bench/internal/bench"
)

func main() {
	var cfg bench.Config
	flag.StringVar(&cfg.Addr, "addr", "http://localhost:9200", "OpenSearch URL, comma separated for several nodes")
	flag.StringVar(&cfg.Username, "user", "", "basic auth username")
	flag.StringVar(&cfg.Password, "pass", "", "basic auth password")
	flag.StringVar(&cfg.Index, "index", "bench", "index to recreate and load")
	flag.StringVar(&cfg.Arm, "arm", "handrolled", "loader: handrolled, bulkindexer, bulkretry (built with -tags bulkretry), or the mget read arm (built with -tags mget)")
	flag.BoolVar(&cfg.Discover, "discover", false, "let the client discover cluster nodes (needs their publish addresses to be reachable)")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 2*time.Minute, "give up on one request attempt after this long (0 waits forever)")
	flag.IntVar(&cfg.Docs, "docs", 1_000_000, "documents per run")
	flag.IntVar(&cfg.Runs, "runs", 5, "measured runs")
	flag.IntVar(&cfg.Warmup, "warmup", 1, "runs to discard before measuring")
	flag.IntVar(&cfg.Shards, "shards", 1, "primary shards")
	flag.IntVar(&cfg.Replicas, "replicas", 0, "replicas per primary shard (write tasks then include the replica copies)")
	flag.StringVar(&cfg.Refresh, "refresh", "-1", "index.refresh_interval during the load")
	flag.IntVar(&cfg.Workers, "workers", 8, "parallel bulk requests in flight")
	flag.IntVar(&cfg.Batch, "batch", 2000, "documents per bulk request (handrolled)")
	flag.IntVar(&cfg.Queue, "queue", 4, "batches buffered ahead of the workers (handrolled)")
	flag.IntVar(&cfg.FlushBytes, "flush-bytes", 5*1024*1024, "flush threshold in bytes (bulkindexer)")
	flag.DurationVar(&cfg.FlushInterval, "flush-interval", 30*time.Second, "flush threshold as duration (bulkindexer)")
	flag.IntVar(&cfg.Producers, "producers", 1, "goroutines calling Add at once (bulkindexer, bulkretry)")
	flag.DurationVar(&cfg.RetryFor, "retry-for", 2*time.Minute, "keep retrying rejected documents this long")
	flag.IntVar(&cfg.MaxTries, "max-attempts", 0, "give up after this many attempts instead of using -retry-for")
	flag.BoolVar(&cfg.NoShardRouter, "no-shard-router", false, "turn the indexer's shard grouping off (bulkretry)")
	flag.BoolVar(&cfg.WarmShardMap, "warm-shard-map", false, "fetch the index's shard map before loading; needs -discover (bulkretry)")
	flag.IntVar(&cfg.MGets, "mgets", 20_000, "mget calls per measured run (mget)")
	flag.IntVar(&cfg.MGetDocs, "mget-docs", 100, "documents per mget (mget)")
	flag.IntVar(&cfg.Readers, "readers", 16, "goroutines calling MGet at once (mget)")
	flag.StringVar(&cfg.MGetDriver, "mget-driver", "coordinator", "coordinator, direct, or both, alternating on one index (mget)")
	flag.BoolVar(&cfg.Alias, "alias", false, "read through an alias of the index, which the client cannot route (mget)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// A read arm loads the index once and then measures reads against it.
	if run, ok := bench.ReadArmFor(cfg.Arm); ok {
		if err := run(ctx, os.Stdout, cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	fmt.Printf("arm=%s docs=%d shards=%d replicas=%d workers=%d runs=%d (+%d warm-up)\n",
		cfg.Arm, cfg.Docs, cfg.Shards, cfg.Replicas, cfg.Workers, cfg.Runs, cfg.Warmup)

	results := make([]bench.Result, 0, cfg.Runs)
	for i := 1; i <= cfg.Warmup+cfg.Runs; i++ {
		res, err := bench.Run(ctx, cfg)
		if err != nil {
			log.Fatalf("run %d: %v", i, err)
		}
		label := "run"
		if i <= cfg.Warmup {
			label = "warm-up (discarded)"
		} else {
			results = append(results, res)
		}
		fmt.Printf("%-20s %8.0f docs/s  in %7s  requests=%-6d write tasks=%-7d rejected=%-6d retried=%-7d dropped=%-5d indexed=%d",
			label, res.DocsPerSec, res.Elapsed.Round(time.Millisecond), res.Requests,
			res.WriteTasks, res.Rejected, res.Retried, res.Dropped, res.Indexed)
		if placed := res.Grouped + res.Ungrouped; placed > 0 {
			fmt.Printf("  grouped=%.0f%%", 100*float64(res.Grouped)/float64(placed))
		}
		fmt.Println()
	}

	bench.Summarize(os.Stdout, cfg, results)
}
