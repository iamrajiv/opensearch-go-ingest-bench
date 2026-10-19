package bench

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v5"
	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
)

// newClient builds a client that keeps enough idle connections for every
// request a loader has in flight. Go keeps only two per host by default, so
// most requests would open a fresh connection. The loaders bound concurrency
// themselves, so the transport does not cap it.
func newClient(cfg Config) (*opensearchapi.Client, error) {
	tp := http.DefaultTransport.(*http.Transport).Clone()
	tp.MaxIdleConnsPerHost = max(cfg.Workers, 64)

	// v5 discovers nodes by default and then talks to the addresses the nodes
	// publish. Behind Docker or a load balancer those are internal addresses
	// the benchmark cannot reach, so discovery is opt-in here.
	discover := cfg.Discover
	interval := time.Duration(-1) // disabled
	if discover {
		interval = 0 // the client's default cadence
	}

	return opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses:             strings.Split(cfg.Addr, ","),
			Username:              cfg.Username,
			Password:              cfg.Password,
			Transport:             tp,
			RetryOnStatus:         []int{502, 503, 504}, // 429 is per document, we handle it
			MaxRetries:            3,
			DiscoverNodesOnStart:  &discover,
			DiscoverNodesInterval: interval,
			// A bulk request that never gets an answer would otherwise hold a
			// worker forever. Timing out turns it into a failed request, which
			// every loader resends, so a stall costs time and not documents.
			RequestTimeout: cfg.RequestTimeout,
		},
	})
}

// prepareIndex recreates the index tuned for a bulk load: no replicas to copy
// to, and no refresh while nobody is searching.
func prepareIndex(ctx context.Context, client *opensearchapi.Client, cfg Config) error {
	// A missing index is fine: this is the first run, or someone cleaned up.
	_, _ = client.Indices.Delete(ctx, &opensearchapi.IndicesDeleteReq{
		Indices: []string{cfg.Index},
	})

	body := fmt.Sprintf(`{"settings":{"index":{
		"number_of_shards":%d,"number_of_replicas":0,"refresh_interval":%q}}}`,
		cfg.Shards, cfg.Refresh)

	_, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index:      cfg.Index,
		BodyReader: strings.NewReader(body),
	})
	return err
}

// finishIndex turns refresh back on, makes everything searchable, and returns
// the number of documents the cluster actually holds.
func finishIndex(ctx context.Context, client *opensearchapi.Client, cfg Config) (int64, error) {
	if _, err := client.Indices.Settings.Put(ctx, &opensearchapi.IndicesPutSettingsReq{
		Indices:    []string{cfg.Index},
		BodyReader: strings.NewReader(`{"index":{"refresh_interval":"1s"}}`),
	}); err != nil {
		return 0, err
	}
	if _, err := client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Indices: []string{cfg.Index},
	}); err != nil {
		return 0, err
	}
	resp, err := client.Count(ctx, &opensearchapi.CountReq{Indices: []string{cfg.Index}})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// writePool is what the cluster's write thread pool had done so far. Tasks are
// the unit the queue holds, so the task count per request is what shard
// grouping changes.
type writePool struct {
	completed int64
	rejected  int64
}

func writeStats(ctx context.Context, client *opensearchapi.Client) (writePool, error) {
	var out writePool
	resp, err := client.Nodes.Stats(ctx, &opensearchapi.NodesStatsReq{
		Metric: []string{"thread_pool"},
	})
	if err != nil {
		return out, err
	}
	for _, node := range resp.Nodes {
		pool, ok := node.ThreadPool["write"]
		if !ok {
			continue
		}
		if pool.Completed != nil {
			out.completed += *pool.Completed
		}
		if pool.Rejected != nil {
			out.rejected += *pool.Rejected
		}
	}
	return out, nil
}
