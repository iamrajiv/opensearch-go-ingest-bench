//go:build mget

// The mget arm needs the mget drivers opensearch-go#1236 merged into main,
// which no release has yet. Build it with -tags mget against a checkout of main
// at 15c8e47 or later; see the README.

package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
)

func init() { readArms["mget"] = runMGet }

// runMGet loads the index once and then measures cfg.Runs phases of cfg.MGets
// mgets, each asking for cfg.MGetDocs random documents, from cfg.Readers
// goroutines at once. Only the driver differs between a coordinator run and a
// direct one: the client, the documents and the order they are asked for are
// the same.
//
// With -mget-driver both, each run measures both drivers on the same index, one
// after the other, in the order coordinator, direct, then direct, coordinator,
// and so on, so a machine that gets busier or quieter during the runs moves both
// drivers' numbers alike.
func runMGet(ctx context.Context, w io.Writer, cfg Config) error {
	drivers, err := mgetDrivers(cfg.MGetDriver)
	if err != nil {
		return err
	}
	obs := &mgetCounts{}
	client, err := newClient(cfg, func(c *opensearchapi.Config) {
		c.MGet = opensearchapi.MGetConfig{Driver: drivers[0], Observer: obs}
	})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer func() { _ = client.Close() }()

	target, err := loadForReads(ctx, client, cfg)
	if err != nil {
		return err
	}
	// The direct driver can only split an mget once the client knows where the
	// index's shards are. Both drivers get the same client, so both wait.
	if cfg.Discover {
		if err := warmShardMap(ctx, client, cfg.Index); err != nil {
			return err
		}
	}

	fmt.Fprintf(w, "arm=mget driver=%s docs=%d mgets=%d docs/mget=%d readers=%d shards=%d replicas=%d alias=%t runs=%d (+%d warm-up)\n",
		cfg.MGetDriver, cfg.Docs, cfg.MGets, cfg.MGetDocs, cfg.Readers, cfg.Shards, cfg.Replicas, cfg.Alias, cfg.Runs, cfg.Warmup)

	results := make(map[opensearchapi.MGetDriver][]ReadResult, len(drivers))
	for i := 1; i <= cfg.Warmup+cfg.Runs; i++ {
		order := drivers
		if i%2 == 0 {
			order = slices.Clone(drivers)
			slices.Reverse(order)
		}
		for _, driver := range order {
			// The same seed for each driver, so both ask for the same documents.
			res, err := readPhase(opensearchapi.WithMGetDriver(ctx, driver), client, cfg, target, obs, uint64(i))
			if err != nil {
				return fmt.Errorf("run %d, %s: %w", i, driver, err)
			}
			label := "run " + driver.String()
			if i <= cfg.Warmup {
				label = "warm-up " + driver.String()
			} else {
				results[driver] = append(results[driver], res)
			}
			PrintRead(w, label, res)
		}
	}

	for _, driver := range drivers {
		dcfg := cfg
		dcfg.MGetDriver = driver.String()
		SummarizeReads(w, dcfg, results[driver])
	}
	return nil
}

// mgetDrivers is the drivers -mget-driver names: one, or both for "both".
func mgetDrivers(name string) ([]opensearchapi.MGetDriver, error) {
	if name == "both" {
		return []opensearchapi.MGetDriver{opensearchapi.MGetDriverCoordinator, opensearchapi.MGetDriverDirect}, nil
	}
	driver, err := opensearchapi.ParseMGetDriver(name)
	if err != nil {
		return nil, err
	}
	if driver == opensearchapi.MGetDriverDefault {
		return nil, fmt.Errorf("-mget-driver must be coordinator, direct or both")
	}
	return []opensearchapi.MGetDriver{driver}, nil
}

// loadForReads fills a fresh index with cfg.Docs documents through the
// hand-rolled loader, checks the index holds every one of them, and returns
// the name to read them through: the index, or an alias of it with cfg.Alias.
func loadForReads(ctx context.Context, client *opensearchapi.Client, cfg Config) (string, error) {
	if err := prepareIndex(ctx, client, cfg); err != nil {
		return "", fmt.Errorf("prepare index: %w", err)
	}

	docs := make(chan Event, cfg.Batch)
	go generate(ctx, cfg.Docs, docs)
	var res Result
	if err := loadHandRolled(ctx, client, cfg, docs, &res); err != nil {
		return "", fmt.Errorf("load: %w", err)
	}

	count, err := finishIndex(ctx, client, cfg)
	if err != nil {
		return "", fmt.Errorf("finish index: %w", err)
	}
	if count != int64(cfg.Docs) {
		return "", fmt.Errorf("index holds %d documents, want %d", count, cfg.Docs)
	}
	// Wait for the replicas too, so no read lands on a copy still recovering.
	if cfg.Replicas > 0 {
		if _, err := client.Cluster.Health(ctx, &opensearchapi.ClusterHealthReq{
			Indices: []string{cfg.Index},
			Params: &opensearchapi.ClusterHealthParams{
				WaitForStatus: "green",
				TimeoutParams: opensearchapi.TimeoutParams{Timeout: 2 * time.Minute},
			},
		}); err != nil {
			return "", fmt.Errorf("wait for replicas: %w", err)
		}
	}

	if !cfg.Alias {
		return cfg.Index, nil
	}
	alias := cfg.Index + "-alias"
	if _, err := client.Indices.PutAlias(ctx, opensearchapi.IndicesPutAliasReq{
		Name:    alias,
		Indices: []string{cfg.Index},
	}); err != nil {
		return "", fmt.Errorf("alias: %w", err)
	}
	return alias, nil
}

// readPhase sends cfg.MGets mgets through target and checks every answer.
func readPhase(ctx context.Context, client *opensearchapi.Client, cfg Config, target string, obs *mgetCounts, seed uint64) (ReadResult, error) {
	var res ReadResult

	before, err := poolStats(ctx, client, "get")
	if err != nil {
		return res, fmt.Errorf("thread pool stats: %w", err)
	}
	obs.reset()

	took := make([]time.Duration, cfg.MGets)
	var next, wrong, failed atomic.Int64
	var once sync.Once
	var firstErr error
	var wg sync.WaitGroup
	start := time.Now()
	for r := range max(cfg.Readers, 1) {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(seed, uint64(r)))
			ids := make([]string, cfg.MGetDocs)
			for {
				n := next.Add(1) - 1
				if n >= int64(cfg.MGets) {
					return
				}
				pickIDs(rng, cfg.Docs, ids)

				t := time.Now()
				resp, err := client.MGet(ctx, mgetReq(target, ids))
				took[n] = time.Since(t)
				if err != nil {
					failed.Add(1)
					wrong.Add(int64(len(ids)))
					once.Do(func() { firstErr = err })
					continue
				}
				wrong.Add(int64(checkMGet(resp, cfg.Index, ids)))
			}
		})
	}
	wg.Wait()
	res.Elapsed = time.Since(start)
	if firstErr != nil {
		fmt.Fprintf(os.Stderr, "first failed mget: %v\n", firstErr)
	}

	after, err := poolStats(ctx, client, "get")
	if err != nil {
		return res, fmt.Errorf("thread pool stats: %w", err)
	}

	res.MGets = int64(cfg.MGets)
	res.DocsPerSec = float64(res.MGets*int64(cfg.MGetDocs)) / res.Elapsed.Seconds()
	res.P50, res.P99 = latencies(took)
	res.Wrong = wrong.Load()
	res.Failed = failed.Load()
	res.GetTasks = after.completed - before.completed
	res.GetRejected = after.rejected - before.rejected
	obs.fill(&res)
	return res, nil
}

// pickIDs fills ids with distinct random documents out of the n loaded.
func pickIDs(rng *rand.Rand, n int, ids []string) {
	seen := make(map[int]struct{}, len(ids))
	for i := range ids {
		k := rng.IntN(n)
		for _, dup := seen[k]; dup; _, dup = seen[k] {
			k = rng.IntN(n)
		}
		seen[k] = struct{}{}
		ids[i] = docID(k)
	}
}

// mgetReq asks for ids by _index and _id, the form the direct driver can split.
// ids in place of docs, or a raw body, would send every mget through a
// coordinating node.
func mgetReq(target string, ids []string) opensearchapi.MGetReq {
	docs := make([]opensearchapi.MGetOperation, len(ids))
	for i, id := range ids {
		docs[i] = opensearchapi.MGetOperation{ID: id, Index: &target}
	}
	return opensearchapi.MGetReq{Body: &opensearchapi.MGetBody{Docs: docs}}
}

// checkMGet counts the documents in resp that are not what was asked for: not
// found, answered with an error, or not in the place of the id asked for. An
// answer of the wrong length counts every document.
func checkMGet(resp *opensearchapi.MGetResp, index string, ids []string) int {
	if len(resp.Docs) != len(ids) {
		return len(ids)
	}
	wrong := 0
	for i := range resp.Docs {
		got, err := resp.Docs[i].GetResult()
		if err != nil || !got.Found || got.ID != ids[i] || got.Index != index {
			wrong++
		}
	}
	return wrong
}

// mgetCounts adds up what the direct driver reports about each mget it splits.
// An mget it does not split, because no document routes to a node, goes to the
// cluster whole and is not reported, and the coordinator driver reports none.
type mgetCounts struct {
	opensearchapi.BaseMGetObserver

	mgets, requests, nodes, retries, coordinatorDocs, resent atomic.Int64
}

func (c *mgetCounts) OnMGetDone(_ context.Context, e opensearchapi.MGetDoneEvent) {
	c.mgets.Add(1)
	c.requests.Add(int64(e.Requests))
	c.nodes.Add(int64(e.Nodes))
	c.retries.Add(int64(e.Retries))
	c.coordinatorDocs.Add(int64(e.CoordinatorDocs))
	if e.Resent {
		c.resent.Add(1)
	}
}

func (c *mgetCounts) reset() {
	for _, n := range []*atomic.Int64{&c.mgets, &c.requests, &c.nodes, &c.retries, &c.coordinatorDocs, &c.resent} {
		n.Store(0)
	}
}

// fill copies the counts into res. Every mget the observer was not told about
// went to the cluster whole, as one request.
func (c *mgetCounts) fill(res *ReadResult) {
	res.Whole = res.MGets - c.mgets.Load()
	res.Requests = c.requests.Load() + res.Whole
	res.Nodes = c.nodes.Load()
	res.Retries = c.retries.Load()
	res.CoordinatorDocs = c.coordinatorDocs.Load()
	res.Resent = c.resent.Load()
}
