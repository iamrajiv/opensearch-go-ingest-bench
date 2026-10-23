//go:build mget

package bench

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/opensearch-project/opensearch-go/v5/opensearchapi"
)

// fakeReads answers _mget like a cluster that holds every document except
// those whose ID ends in 7, and counts each mget as one get task.
type fakeReads struct {
	mu      sync.Mutex
	mgets   int64
	missing int64
}

func (f *fakeReads) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(r.URL.Path, "/_mget"):
		var body struct {
			Docs []struct {
				ID    string `json:"_id"`
				Index string `json:"_index"`
			} `json:"docs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.mgets++
		docs := make([]map[string]any, len(body.Docs))
		for i, d := range body.Docs {
			found := !strings.HasSuffix(d.ID, "7")
			if !found {
				f.missing++
			}
			docs[i] = map[string]any{"_index": d.Index, "_id": d.ID, "found": found}
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"docs": docs})
	case strings.HasPrefix(r.URL.Path, "/_nodes/stats"):
		f.mu.Lock()
		completed := f.mgets
		f.mu.Unlock()
		fmt.Fprintf(w, `{"_nodes":{"total":1,"successful":1,"failed":0},"cluster_name":"fake",`+
			`"nodes":{"n1":{"thread_pool":{"get":{"completed":%d,"rejected":0}}}}}`, completed)
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

// A read phase through the coordinator driver sends every mget whole, as one
// request, and counts exactly the documents the cluster could not find as wrong.
func TestReadPhaseCountsWhatTheClusterMissed(t *testing.T) {
	fake := &fakeReads{}
	srv := httptest.NewServer(fake)
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Docs, cfg.MGets, cfg.MGetDocs, cfg.Readers = 1000, 50, 20, 4
	obs := &mgetCounts{}
	client, err := newClient(cfg, func(c *opensearchapi.Config) {
		c.MGet = opensearchapi.MGetConfig{Driver: opensearchapi.MGetDriverCoordinator, Observer: obs}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	res, err := readPhase(t.Context(), client, cfg, cfg.Index, obs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.MGets != 50 || res.Requests != 50 || res.Whole != 50 || res.Failed != 0 {
		t.Errorf("got %d mgets, %d requests, %d sent whole, %d failed; want 50, 50, 50 and 0",
			res.MGets, res.Requests, res.Whole, res.Failed)
	}
	if res.Wrong != fake.missing || fake.missing == 0 {
		t.Errorf("got %d wrong, the cluster missed %d", res.Wrong, fake.missing)
	}
	if res.GetTasks != 50 {
		t.Errorf("got %d get tasks, want one per mget", res.GetTasks)
	}
}

func TestCheckMGet(t *testing.T) {
	ids := []string{"a", "b", "c"}
	tests := []struct {
		name string
		resp string
		want int
	}{
		{"all found in order", `{"docs":[
			{"_index":"bench","_id":"a","found":true},
			{"_index":"bench","_id":"b","found":true},
			{"_index":"bench","_id":"c","found":true}]}`, 0},
		{"one missing, two swapped", `{"docs":[
			{"_index":"bench","_id":"a","found":false},
			{"_index":"bench","_id":"c","found":true},
			{"_index":"bench","_id":"b","found":true}]}`, 3},
		{"another index", `{"docs":[
			{"_index":"other","_id":"a","found":true},
			{"_index":"bench","_id":"b","found":true},
			{"_index":"bench","_id":"c","found":true}]}`, 1},
		{"an error in place of a document", `{"docs":[
			{"_index":"bench","_id":"a","found":true},
			{"_index":"bench","_id":"b","error":{"type":"x","reason":"y"}},
			{"_index":"bench","_id":"c","found":true}]}`, 1},
		{"short answer", `{"docs":[{"_index":"bench","_id":"a","found":true}]}`, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp opensearchapi.MGetResp
			if err := json.Unmarshal([]byte(tt.resp), &resp); err != nil {
				t.Fatal(err)
			}
			if got := checkMGet(&resp, "bench", ids); got != tt.want {
				t.Errorf("got %d wrong, want %d", got, tt.want)
			}
		})
	}
}

func TestPickIDsAreDistinct(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	ids := make([]string, 50)
	pickIDs(rng, 50, ids) // every document, so a repeat would have to be redrawn
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("%s picked twice", id)
		}
		seen[id] = true
	}
}

func TestMGetDrivers(t *testing.T) {
	both, err := mgetDrivers("both")
	if err != nil || len(both) != 2 || both[0] != opensearchapi.MGetDriverCoordinator || both[1] != opensearchapi.MGetDriverDirect {
		t.Errorf("both: got %v, %v", both, err)
	}
	if one, err := mgetDrivers("direct"); err != nil || len(one) != 1 || one[0] != opensearchapi.MGetDriverDirect {
		t.Errorf("direct: got %v, %v", one, err)
	}
	for _, name := range []string{"", "fast"} {
		if _, err := mgetDrivers(name); err == nil {
			t.Errorf("%q: want an error", name)
		}
	}
}
