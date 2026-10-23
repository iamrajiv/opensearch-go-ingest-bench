package bench

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestLatencies(t *testing.T) {
	d := make([]time.Duration, 100)
	for i := range d {
		d[i] = time.Duration(100-i) * time.Millisecond // unsorted on purpose
	}
	p50, p99 := latencies(d)
	if p50 != 51*time.Millisecond || p99 != 100*time.Millisecond {
		t.Errorf("got p50=%v p99=%v, want 51ms and 100ms", p50, p99)
	}
	if p50, p99 := latencies(nil); p50 != 0 || p99 != 0 {
		t.Errorf("empty: got p50=%v p99=%v", p50, p99)
	}
}

func TestPerMGet(t *testing.T) {
	if got := perMGet(30, 10); got != 3 {
		t.Errorf("got %v, want 3", got)
	}
	if got := perMGet(30, 0); got != 0 {
		t.Errorf("no mgets: got %v, want 0", got)
	}
}

// A run that read one wrong document is not a correct run, and the summary
// says so.
func TestSummarizeReadsCountsCorrectRuns(t *testing.T) {
	cfg := Config{Arm: "mget", MGetDriver: "direct"}
	runs := []ReadResult{
		{DocsPerSec: 100, MGets: 10, P50: time.Millisecond, P99: 2 * time.Millisecond},
		{DocsPerSec: 120, MGets: 10, P50: time.Millisecond, P99: 2 * time.Millisecond, Wrong: 1},
	}

	var out bytes.Buffer
	SummarizeReads(&out, cfg, runs)
	if !strings.Contains(out.String(), "1/2") {
		t.Errorf("summary does not count one correct run of two:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "WARNING: 1 of 2 runs") {
		t.Errorf("summary does not warn about the wrong run:\n%s", out.String())
	}
}

func TestReadArmForLoader(t *testing.T) {
	if _, ok := ReadArmFor("handrolled"); ok {
		t.Error("handrolled is a loader, not a read arm")
	}
}
