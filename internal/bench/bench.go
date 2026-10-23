// Package bench measures OpenSearch ingestion: how fast a loader goes, and
// whether it loses anything while doing it.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
)

// Config is one measurement setup: which loader, how much data, which knobs.
type Config struct {
	Addr     string
	Username string
	Password string
	Index    string
	Arm      string // handrolled | bulkindexer | bulkretry (with -tags bulkretry)
	Discover bool   // let the client discover nodes; needs reachable publish addresses

	RequestTimeout time.Duration // per attempt; 0 waits forever

	Docs     int
	Runs     int
	Warmup   int
	Shards   int
	Replicas int    // number_of_replicas; write tasks then include the copies
	Refresh  string // index.refresh_interval during the load

	// Shared loader knobs.
	Workers  int
	Batch    int
	RetryFor time.Duration
	MaxTries int // non-zero swaps the time window for a fixed attempt count

	// BulkIndexer only.
	FlushBytes    int
	FlushInterval time.Duration
	Producers     int // goroutines calling Add at once

	// Hand-rolled only.
	Queue int

	// bulkretry only.
	NoShardRouter bool // turn the indexer's shard grouping off
	WarmShardMap  bool // fetch the index's shard map before loading
}

// Result is what one run produced. Counts come from the loader; Indexed comes
// from the index itself, so a run proves what it stored rather than claiming it.
type Result struct {
	Elapsed    time.Duration
	DocsPerSec float64
	Requests   int64
	Retried    int64
	Dropped    int64 // given up on: the documents a careless loader would lose
	Indexed    int64 // counted from the index after a refresh
	WriteTasks int64 // write thread pool tasks the cluster had to run
	Rejected   int64 // write thread pool rejections during the run

	// Items the indexer placed in a shard buffer, and those it could not
	// (bulkretry only).
	Grouped   int64
	Ungrouped int64
}

// Loader is one way of getting documents into OpenSearch. It owns retries and
// fills in Requests, Retried and Dropped.
type Loader func(ctx context.Context, client *opensearchapi.Client, cfg Config, docs <-chan Event, res *Result) error

// Run executes a single measured run: fresh index, load, count, report.
func Run(ctx context.Context, cfg Config) (Result, error) {
	var res Result

	client, err := newClient(cfg)
	if err != nil {
		return res, fmt.Errorf("client: %w", err)
	}
	defer func() { _ = client.Close() }() // v5 clients own goroutines; release them per run

	if err := prepareIndex(ctx, client, cfg); err != nil {
		return res, fmt.Errorf("prepare index: %w", err)
	}

	before, err := poolStats(ctx, client, "write")
	if err != nil {
		return res, fmt.Errorf("thread pool stats: %w", err)
	}

	loader, err := loaderFor(cfg.Arm)
	if err != nil {
		return res, err
	}

	docs := make(chan Event, cfg.Batch)
	go generate(ctx, cfg.Docs, docs)

	start := time.Now()
	if err := loader(ctx, client, cfg, docs, &res); err != nil {
		return res, fmt.Errorf("load: %w", err)
	}
	res.Elapsed = time.Since(start)
	res.DocsPerSec = float64(cfg.Docs) / res.Elapsed.Seconds()

	after, err := poolStats(ctx, client, "write")
	if err != nil {
		return res, fmt.Errorf("thread pool stats: %w", err)
	}
	res.WriteTasks = after.completed - before.completed
	res.Rejected = after.rejected - before.rejected

	count, err := finishIndex(ctx, client, cfg)
	if err != nil {
		return res, fmt.Errorf("finish index: %w", err)
	}
	res.Indexed = count

	return res, nil
}

// loaders holds every arm this build has. bulkretry registers itself when
// built with its tag.
var loaders = map[string]Loader{
	"handrolled":  loadHandRolled,
	"bulkindexer": loadBulkIndexer,
}

func loaderFor(arm string) (Loader, error) {
	load, ok := loaders[arm]
	if !ok {
		names := make([]string, 0, len(loaders))
		for name := range loaders {
			names = append(names, name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("unknown arm %q: this build has %v", arm, names)
	}
	return load, nil
}

// retryable reports whether a per-item status is worth sending again. 429 means
// the write queue was full, 503 a node was briefly unavailable. Everything else
// fails the same way next time.
func retryable(status int) bool { return status == 429 || status == 503 }

const (
	baseDelay = 200 * time.Millisecond
	maxDelay  = 10 * time.Second
)

// backoff grows the wait between attempts and spreads workers out, so they do
// not all return to a busy cluster at the same instant.
func backoff(attempt int) time.Duration {
	d := baseDelay << (attempt - 1)
	if d <= 0 || d > maxDelay { // d <= 0 catches the shift overflowing
		d = maxDelay
	}
	return baseDelay/2 + time.Duration(rand.Int64N(int64(d)))
}

// keepTrying is the give-up rule. The default is a time window: a 429 means the
// cluster is busy, and busy passes with time, not with attempts.
func keepTrying(cfg Config, attempt int, started time.Time) bool {
	if cfg.MaxTries > 0 {
		return attempt < cfg.MaxTries
	}
	return time.Since(started) < cfg.RetryFor
}

// addAll calls add for every event from in, from several goroutines at once,
// the way a service with many request handlers feeds one shared indexer.
func addAll(in <-chan Event, producers int, add func(Event) error) error {
	var wg sync.WaitGroup
	errs := make([]error, max(producers, 1))
	for i := range errs {
		wg.Go(func() {
			for ev := range in {
				if err := add(ev); err != nil {
					errs[i] = err
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// Summarize prints the median and the full spread, never a single run.
func Summarize(w io.Writer, cfg Config, runs []Result) {
	if len(runs) == 0 {
		return
	}
	rate := make([]float64, len(runs))
	for i, r := range runs {
		rate[i] = r.DocsPerSec
	}
	sort.Float64s(rate)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "\narm\truns\tdocs/s\tspread\trequests\twrite tasks\trejected\tretried\tdropped\tlossless\n")

	var requests, tasks, rejected, retried, dropped, lossless int64
	for _, r := range runs {
		requests += r.Requests
		tasks += r.WriteTasks
		rejected += r.Rejected
		retried += r.Retried
		dropped += r.Dropped
		if r.Indexed == int64(cfg.Docs) {
			lossless++
		}
	}
	n := int64(len(runs))
	fmt.Fprintf(tw, "%s\t%d\t%.0f\t%.0f - %.0f\t%d\t%d\t%d\t%d\t%d\t%d/%d\n",
		cfg.Arm, len(runs), median(rate), rate[0], rate[len(rate)-1],
		requests/n, tasks/n, rejected/n, retried/n, dropped/n, lossless, n)
	tw.Flush()

	if lossless != n {
		fmt.Fprintf(w, "\nWARNING: %d of %d runs did not end with exactly %d documents in the index\n",
			n-lossless, n, cfg.Docs)
	}
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
