package bench

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// Event is the document every loader sends: a synthetic log line of roughly
// 200 bytes, which is typical for logs and metrics payloads.
type Event struct {
	ID        string    `json:"-"`
	Timestamp time.Time `json:"@timestamp"`
	Service   string    `json:"service"`
	Level     string    `json:"level"`
	Region    string    `json:"region"`
	LatencyMS int       `json:"latency_ms"`
	Message   string    `json:"message"`
}

var (
	services = []string{"booking", "tracking", "billing", "customs", "vessel"}
	levels   = []string{"INFO", "INFO", "INFO", "WARN", "ERROR"}
	regions  = []string{"eu-west-1", "us-west-2", "ap-south-1"}
)

// docID is the ID of the i-th generated event, which the read arms ask for.
func docID(i int) string { return fmt.Sprintf("evt-%09d", i) }

// generate produces n events into out and closes it. The ID is ours, not the
// cluster's, so re-sending a document replaces it instead of duplicating it.
func generate(ctx context.Context, n int, out chan<- Event) {
	defer close(out)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range n {
		ev := Event{
			ID:        docID(i),
			Timestamp: time.Now(),
			Service:   services[r.IntN(len(services))],
			Level:     levels[r.IntN(len(levels))],
			Region:    regions[r.IntN(len(regions))],
			LatencyMS: r.IntN(900) + 5,
			Message:   fmt.Sprintf("request %d processed", i),
		}
		select {
		case out <- ev:
		case <-ctx.Done():
			return
		}
	}
}
