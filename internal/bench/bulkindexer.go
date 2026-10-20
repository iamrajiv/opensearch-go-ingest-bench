package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v5/opensearchutil"
)

// loadBulkIndexer uses the client's own helper, which batches and runs the
// worker pool for you. It does not retry rejected documents, so the retry
// rounds here are the caller's job, as the client's error handling guide says.
//
// The first pass streams straight from the generator, so its timing is
// comparable with the hand-rolled loader. Later passes resend only what the
// cluster rejected with a retryable status.
func loadBulkIndexer(ctx context.Context, client *opensearchapi.Client, cfg Config, docs <-chan Event, res *Result) error {
	var c counters
	started := time.Now()

	failed, err := indexPass(ctx, client, cfg, docs, &c)
	if err != nil {
		return err
	}

	for attempt := 1; len(failed) > 0; attempt++ {
		if ctx.Err() != nil {
			c.dropped.Add(int64(len(failed)))
			break
		}
		var pending []Event
		for _, ev := range failed {
			if keepTrying(cfg, attempt, started) {
				pending = append(pending, ev)
				continue
			}
			c.dropped.Add(1)
		}
		if len(pending) == 0 {
			break
		}
		c.retried.Add(int64(len(pending)))
		sleep(ctx, backoff(attempt))

		in := make(chan Event, len(pending))
		for _, ev := range pending {
			in <- ev
		}
		close(in)
		if failed, err = indexPass(ctx, client, cfg, in, &c); err != nil {
			return err
		}
	}

	res.Requests = c.requests.Load()
	res.Retried = c.retried.Load()
	res.Dropped = c.dropped.Load()
	return nil
}

// indexPass sends everything from in through a fresh BulkIndexer and returns
// the documents worth sending again. A fresh indexer per pass is deliberate:
// an indexer cannot be reused after Close.
func indexPass(ctx context.Context, client *opensearchapi.Client, cfg Config, in <-chan Event, c *counters) ([]Event, error) {
	bi, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:        client,
		Index:         cfg.Index,
		NumWorkers:    cfg.Workers,
		FlushBytes:    cfg.FlushBytes,
		FlushInterval: cfg.FlushInterval,
	})
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	var retry []Event

	addErr := addAll(in, cfg.Producers, func(ev Event) error {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		return bi.Add(ctx, opensearchutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: ev.ID,
			Body:       bytes.NewReader(body),
			// Per-document rejections arrive with a nil error and the status
			// in resp. A failed request arrives with a non-nil error and an
			// empty resp, so status 0 here means "unknown", not "fine".
			OnFailure: func(_ context.Context, _ opensearchutil.BulkIndexerItem, resp opensearchapi.BulkRespItem, err error) {
				if err != nil || retryable(resp.Status) {
					mu.Lock()
					retry = append(retry, ev)
					mu.Unlock()
					return
				}
				c.dropped.Add(1)
			},
		})
	})
	// Close even when Add failed, so the indexer's goroutines are released.
	if err := errors.Join(addErr, bi.Close(ctx)); err != nil {
		return nil, err
	}
	c.requests.Add(int64(bi.Stats().NumRequests))

	mu.Lock()
	defer mu.Unlock()
	return retry, nil
}
