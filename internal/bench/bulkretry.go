//go:build bulkretry

// The bulkretry arm needs the BulkIndexer from opensearch-go#1219 (RetryPolicy,
// shard grouping), which no release has yet. Build it with -tags bulkretry
// against a checkout of that branch; see the README.

package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v5/opensearchtransport"
	"github.com/opensearch-project/opensearch-go/v5/opensearchutil"
)

func init() { loaders["bulkretry"] = loadBulkRetry }

// loadBulkRetry hands retries to the indexer itself and keeps one indexer for
// the whole load, as the indexer's guide asks. The give-up rule and backoff are
// the same as the other arms, so only the indexer differs.
func loadBulkRetry(ctx context.Context, client *opensearchapi.Client, cfg Config, docs <-chan Event, res *Result) error {
	var c counters
	started := time.Now()

	if cfg.WarmShardMap {
		if err := warmShardMap(ctx, client, cfg.Index); err != nil {
			return err
		}
	}

	// Every id is deterministic, so a transport failure is safe to resend too.
	policy := opensearchutil.RetryPolicyFunc(func(attempt int, _ opensearchutil.FailureScope,
		kind opensearchutil.FailureKind, _ opensearchapi.BulkRespItem, _ error,
	) (time.Duration, bool) {
		if kind == opensearchutil.FailurePermanent || !keepTrying(cfg, attempt, started) {
			return 0, false
		}
		return backoff(attempt), true
	})

	icfg := opensearchutil.BulkIndexerConfig{
		Client:        client,
		Index:         cfg.Index,
		NumWorkers:    cfg.Workers,
		FlushBytes:    cfg.FlushBytes,
		FlushInterval: cfg.FlushInterval,
		RetryPolicy:   policy,
		Timeout:       time.Minute, // the guide asks for a server-side timeout with a policy
		OnWarning: func(_ context.Context, err error) {
			log.Printf("bulk indexer warning: %v", err)
		},
	}
	if cfg.NoShardRouter {
		icfg.ShardRouter = opensearchutil.NoShardRouter
	}
	bi, err := opensearchutil.NewBulkIndexer(icfg)
	if err != nil {
		return err
	}

	addErr := func() error {
		for ev := range docs {
			body, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			err = bi.Add(ctx, opensearchutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: ev.ID,
				Body:       bytes.NewReader(body),
				// Fires once, when the item is final: the policy gave up or
				// the failure was permanent.
				OnFailure: func(context.Context, opensearchutil.BulkIndexerItem, opensearchapi.BulkRespItem, error) {
					c.dropped.Add(1)
				},
			})
			if err != nil {
				return err
			}
		}
		return nil
	}()
	// Close even when Add failed: with a cancelled context it cancels what is
	// in flight instead of waiting, and it releases the indexer's goroutines.
	if err := errors.Join(addErr, bi.Close(ctx)); err != nil {
		return err
	}

	st := bi.Stats()
	res.Requests = int64(st.NumRequests)
	for _, n := range st.NumRetries {
		res.Retried += int64(n)
	}
	res.Dropped = c.dropped.Load()
	res.Grouped = int64(st.NumGroupedItems)
	res.Ungrouped = int64(st.NumUngroupedItems)
	return nil
}

// warmShardMap gets the index's shard map into the client before the load. A
// lookup registers the index and the next discovery cycle fetches its map, so
// without this a fresh index goes ungrouped until that cycle comes round.
func warmShardMap(ctx context.Context, client *opensearchapi.Client, index string) error {
	tr, ok := client.Client.Transport.(*opensearchtransport.Transport)
	if !ok {
		return fmt.Errorf("warm shard map: transport is %T", client.Client.Transport)
	}
	if err := tr.ShardMapUnavailable(); err != nil {
		return fmt.Errorf("warm shard map: %w", err)
	}
	tr.ShardMap(index) // registers the index
	if err := client.Client.DiscoverNodes(ctx); err != nil {
		return fmt.Errorf("warm shard map: %w", err)
	}
	if _, ok := tr.ShardMap(index); !ok {
		return fmt.Errorf("warm shard map: %s still unknown after discovery", index)
	}
	return nil
}
