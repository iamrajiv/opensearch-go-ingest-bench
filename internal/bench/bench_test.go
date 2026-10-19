package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCluster answers _bulk like a busy OpenSearch: every document is rejected
// with 429 the first time it is seen and accepted after that, and documents in
// bad are rejected with 400 every time. Any other path answers 200 {}.
type fakeCluster struct {
	mu       sync.Mutex
	seen     map[string]bool
	stored   map[string]bool
	bad      map[string]bool
	requests int
}

func newFakeCluster(bad ...string) *fakeCluster {
	f := &fakeCluster{seen: map[string]bool{}, stored: map[string]bool{}, bad: map[string]bool{}}
	for _, id := range bad {
		f.bad[id] = true
	}
	return f
}

func (f *fakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !strings.HasSuffix(r.URL.Path, "/_bulk") {
		_, _ = w.Write([]byte(`{}`))
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++

	type item map[string]any
	var items []map[string]item
	anyErr := false

	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var action map[string]struct {
			ID string `json:"_id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &action); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sc.Scan() // the document line
		id := action["index"].ID

		res := item{"_index": "bench", "_id": id}
		switch {
		case f.bad[id]:
			anyErr = true
			res["status"] = 400
			res["error"] = map[string]string{"type": "mapper_parsing_exception", "reason": "bad document"}
		case !f.seen[id]:
			anyErr = true
			f.seen[id] = true
			res["status"] = 429
			res["error"] = map[string]string{"type": "rejected_execution_exception", "reason": "queue full"}
		default:
			f.stored[id] = true
			res["status"] = 201
			res["result"] = "created"
		}
		items = append(items, map[string]item{"index": res})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"took": 1, "errors": anyErr, "items": items})
}

func (f *fakeCluster) storedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stored)
}

func testConfig(addr string) Config {
	return Config{
		Addr: addr, Index: "bench", Workers: 4, Batch: 10, Queue: 2,
		FlushBytes: 1024, FlushInterval: 50 * time.Millisecond, RetryFor: 30 * time.Second,
	}
}

func events(n int) <-chan Event {
	ch := make(chan Event, n)
	for i := range n {
		ch <- Event{ID: fmt.Sprintf("evt-%09d", i), Message: "x"}
	}
	close(ch)
	return ch
}

// Every document is rejected once with 429. Every loader must retry and end
// with every document stored and nothing dropped.
func TestLoadersRecoverEvery429(t *testing.T) {
	for name, load := range loaders {
		t.Run(name, func(t *testing.T) {
			fake := newFakeCluster()
			srv := httptest.NewServer(fake)
			defer srv.Close()

			cfg := testConfig(srv.URL)
			client, err := newClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()

			const n = 60
			var res Result
			if err := load(context.Background(), client, cfg, events(n), &res); err != nil {
				t.Fatal(err)
			}
			if got := fake.storedCount(); got != n {
				t.Errorf("stored %d documents, want %d", got, n)
			}
			if res.Dropped != 0 {
				t.Errorf("dropped %d, want 0", res.Dropped)
			}
			if res.Retried != n {
				t.Errorf("retried %d, want %d: each document is rejected exactly once", res.Retried, n)
			}
		})
	}
}

// A bad document is never retried and is counted as dropped, so a loader
// cannot report success while the index is short.
func TestLoadersCountPermanentFailures(t *testing.T) {
	for name, load := range loaders {
		t.Run(name, func(t *testing.T) {
			fake := newFakeCluster("evt-000000003", "evt-000000007")
			srv := httptest.NewServer(fake)
			defer srv.Close()

			cfg := testConfig(srv.URL)
			client, err := newClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()

			const n = 20
			var res Result
			if err := load(context.Background(), client, cfg, events(n), &res); err != nil {
				t.Fatal(err)
			}
			if res.Dropped != 2 {
				t.Errorf("dropped %d, want 2", res.Dropped)
			}
			if got := fake.storedCount(); got != n-2 {
				t.Errorf("stored %d, want %d", got, n-2)
			}
		})
	}
}

// With a one-attempt give-up rule, a cluster that rejects everything once
// costs every document. This is the failure the time window exists to avoid.
func TestAttemptCountGivesUpOnBusyCluster(t *testing.T) {
	fake := newFakeCluster()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.MaxTries = 1
	client, err := newClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	const n = 30
	var res Result
	if err := loadHandRolled(context.Background(), client, cfg, events(n), &res); err != nil {
		t.Fatal(err)
	}
	if res.Dropped != n || fake.storedCount() != 0 {
		t.Errorf("dropped %d stored %d, want %d and 0", res.Dropped, fake.storedCount(), n)
	}
}

func TestBackoffStaysInBounds(t *testing.T) {
	for attempt := 1; attempt <= 80; attempt++ { // 80 also exercises the shift overflow
		for range 50 {
			d := backoff(attempt)
			if d < baseDelay/2 || d > baseDelay/2+maxDelay {
				t.Fatalf("attempt %d: backoff %v outside [%v, %v]", attempt, d, baseDelay/2, baseDelay/2+maxDelay)
			}
		}
	}
}

func TestRetryable(t *testing.T) {
	for status, want := range map[int]bool{429: true, 503: true, 400: false, 404: false, 409: false, 500: false, 0: false} {
		if got := retryable(status); got != want {
			t.Errorf("retryable(%d) = %v, want %v", status, got, want)
		}
	}
}

func TestKeepTrying(t *testing.T) {
	window := Config{RetryFor: time.Hour}
	if !keepTrying(window, 1000, time.Now()) {
		t.Error("time window: should keep trying regardless of attempt count")
	}
	if keepTrying(Config{RetryFor: time.Millisecond}, 1, time.Now().Add(-time.Second)) {
		t.Error("time window: should stop once the window has passed")
	}
	attempts := Config{MaxTries: 3}
	if !keepTrying(attempts, 2, time.Now()) || keepTrying(attempts, 3, time.Now()) {
		t.Error("attempt count: should allow attempts 1 and 2, stop at 3")
	}
}

func TestEncodeBulk(t *testing.T) {
	buf, err := encodeBulk([]Event{{ID: "a", Service: "s1"}, {ID: "b", Service: "s2"}})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want 4 (action and document per event)", len(lines))
	}
	if lines[0] != `{"index":{"_id":"a"}}` || lines[2] != `{"index":{"_id":"b"}}` {
		t.Errorf("action lines carry the wrong ids: %q %q", lines[0], lines[2])
	}
	if !bytes.Contains([]byte(lines[1]), []byte(`"service":"s1"`)) || bytes.Contains([]byte(lines[1]), []byte(`"ID"`)) {
		t.Errorf("document line should hold the fields and not the id: %s", lines[1])
	}
}

func TestBatcherFlushesBySizeTimeAndClose(t *testing.T) {
	in := make(chan Event)
	out := make(chan []Event, 10)
	go batcher(context.Background(), in, out, 3, 50*time.Millisecond)

	for range 4 { // one full batch of 3, one straggler
		in <- Event{}
	}
	if b := <-out; len(b) != 3 {
		t.Fatalf("size flush: got %d, want 3", len(b))
	}
	select {
	case b := <-out:
		if len(b) != 1 {
			t.Fatalf("timer flush: got %d, want 1", len(b))
		}
	case <-time.After(time.Second):
		t.Fatal("timer flush never happened")
	}
	in <- Event{}
	close(in)
	if b := <-out; len(b) != 1 {
		t.Fatalf("close flush: got %d, want 1", len(b))
	}
	if _, ok := <-out; ok {
		t.Fatal("batcher should close its output")
	}
}

func TestMedian(t *testing.T) {
	if m := median([]float64{1, 2, 9}); m != 2 {
		t.Errorf("odd: got %v", m)
	}
	if m := median([]float64{1, 2, 3, 9}); m != 2.5 {
		t.Errorf("even: got %v", m)
	}
}
