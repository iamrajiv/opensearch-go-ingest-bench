//go:build bulkretry

package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v5/opensearchtransport"
)

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
	// Read the placement once the primaries have started (yellow; green would
	// also wait for replicas). Read while they are still initializing, it
	// names no node for them until the next cycle.
	if _, err := client.Cluster.Health(ctx, &opensearchapi.ClusterHealthReq{
		Indices: []string{index},
		Params: &opensearchapi.ClusterHealthParams{
			WaitForStatus: "yellow",
			TimeoutParams: opensearchapi.TimeoutParams{Timeout: 30 * time.Second},
		},
	}); err != nil {
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
