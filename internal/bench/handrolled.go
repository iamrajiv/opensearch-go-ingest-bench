package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
)

// loadHandRolled is the pipeline a service writes itself: generate, batch,
// send with a fixed pool, and retry only the documents the cluster rejected.
//
//	generate -> [chan Event] -> batcher -> [chan []Event] -> N workers -> _bulk
func loadHandRolled(ctx context.Context, client *opensearchapi.Client, cfg Config, docs <-chan Event, res *Result) error {
	batches := make(chan []Event, cfg.Queue)
	go batcher(ctx, docs, batches, cfg.Batch, 500*time.Millisecond)

	var c counters
	var wg sync.WaitGroup
	for range cfg.Workers {
		wg.Go(func() {
			for b := range batches {
				indexBatch(ctx, client, cfg, b, &c)
			}
		})
	}
	wg.Wait()

	res.Requests = c.requests.Load()
	res.Retried = c.retried.Load()
	res.Dropped = c.dropped.Load()
	return nil
}

// counters are shared by every worker, so they are atomic.
type counters struct {
	requests atomic.Int64
	retried  atomic.Int64
	dropped  atomic.Int64
}

// batcher groups events into batches of size, or sends whatever has arrived
// when flush elapses, so a slow source never leaves a partial batch waiting.
// Sending blocks while every worker is busy: that is the backpressure.
func batcher(ctx context.Context, in <-chan Event, out chan<- []Event, size int, flush time.Duration) {
	defer close(out)
	buf := make([]Event, 0, size)
	t := time.NewTicker(flush)
	defer t.Stop()

	emit := func() {
		if len(buf) == 0 {
			return
		}
		select {
		case out <- buf:
		case <-ctx.Done():
		}
		buf = make([]Event, 0, size)
	}
	for {
		select {
		case ev, ok := <-in:
			if !ok {
				emit()
				return
			}
			buf = append(buf, ev)
			if len(buf) == size {
				emit()
			}
		case <-t.C:
			emit()
		case <-ctx.Done():
			return
		}
	}
}

// indexBatch sends a batch and keeps re-sending only the rejected documents,
// backing off between rounds, until they are accepted or the give-up rule says
// stop. Nothing is dropped silently: whatever is given up on is counted.
func indexBatch(ctx context.Context, client *opensearchapi.Client, cfg Config, batch []Event, c *counters) {
	started := time.Now()
	for attempt := 1; len(batch) > 0; attempt++ {
		if ctx.Err() != nil { // Ctrl-C: stop rather than spin through the window
			c.dropped.Add(int64(len(batch)))
			return
		}

		c.requests.Add(1)
		retry, permanent, err := sendBulk(ctx, client, cfg.Index, batch)
		if err != nil {
			// The request itself failed, so every document in it is unknown.
			// Re-sending is safe because the IDs are ours.
			retry, permanent = batch, 0
		}
		c.dropped.Add(int64(permanent))

		next := batch[:0:0] // fresh backing array: retry may alias batch
		for _, ev := range retry {
			if keepTrying(cfg, attempt, started) {
				next = append(next, ev)
				continue
			}
			c.dropped.Add(1)
		}
		batch = next
		if len(batch) > 0 {
			c.retried.Add(int64(len(batch)))
			sleep(ctx, backoff(attempt))
		}
	}
}

// sendBulk sends one batch. It returns the documents worth sending again, the
// number rejected for good, and an error only when the request itself failed.
//
// In v5 a partial failure comes back as a *PartialBulkError while the HTTP
// status is still 200, so the error is the thing to read, not the status code.
func sendBulk(ctx context.Context, client *opensearchapi.Client, index string, batch []Event) ([]Event, int, error) {
	body, err := encodeBulk(batch)
	if err != nil {
		return nil, 0, err
	}

	_, err = client.Bulk(ctx, opensearchapi.BulkReq{Index: index, Body: body})
	if err == nil {
		return nil, 0, nil
	}

	byID := make(map[string]Event, len(batch))
	for _, ev := range batch {
		byID[ev.ID] = ev
	}

	var retry []Event
	permanent := 0
	for _, sub := range opensearchapi.Errors(err) {
		var partial *opensearchapi.PartialBulkError
		if !errors.As(sub, &partial) {
			return nil, 0, err // transport, HTTP or decode failure: caller resends
		}
		for _, item := range partial.FailedItems {
			ev, ok := lookup(byID, item.ID)
			switch {
			case !ok:
				permanent++ // the cluster named a document we did not send
			case retryable(item.Status):
				retry = append(retry, ev)
			default:
				permanent++ // a bad document fails the same way every time
			}
		}
	}
	return retry, permanent, nil
}

func lookup(byID map[string]Event, id *string) (Event, bool) {
	if id == nil {
		return Event{}, false
	}
	ev, ok := byID[*id]
	return ev, ok
}

// encodeBulk turns a batch into the NDJSON the _bulk API expects: one action
// line carrying our own document ID, then one line with the document.
func encodeBulk(batch []Event) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	for _, ev := range batch {
		fmt.Fprintf(buf, `{"index":{"_id":%q}}`+"\n", ev.ID)
		if err := enc.Encode(ev); err != nil { // Encode appends the newline
			return nil, err
		}
	}
	return buf, nil
}
