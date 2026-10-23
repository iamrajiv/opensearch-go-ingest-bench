package bench

import (
	"context"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"
	"time"
)

// ReadResult is what one measured read phase produced. Wrong comes from
// checking every answer against the document asked for, so a run proves what
// it read rather than claiming it.
type ReadResult struct {
	Elapsed    time.Duration
	DocsPerSec float64
	MGets      int64
	P50, P99   time.Duration // one mget, as the caller waits for it
	Wrong      int64         // documents missing, out of place, or answered with an error
	Failed     int64         // mgets that returned an error

	// From the client's mget observer, which is told about the mgets the direct
	// driver splits and about no others.
	Requests        int64 // requests sent, retries and coordinator requests included
	Whole           int64 // mgets sent whole to a coordinating node, unsplit
	Nodes           int64 // nodes sent requests directly, summed over every mget
	Retries         int64 // requests that carried documents a second time
	CoordinatorDocs int64 // documents of split mgets answered through a coordinating node
	Resent          int64 // split mgets sent again whole through a coordinating node

	GetTasks    int64 // get thread pool tasks the cluster had to run
	GetRejected int64 // get thread pool rejections during the run
}

// ReadArm runs a whole read benchmark: it loads the index once, then measures
// cfg.Runs read phases against it and prints them.
type ReadArm func(ctx context.Context, w io.Writer, cfg Config) error

// readArms holds every read arm this build has. mget registers itself when
// built with its tag.
var readArms = map[string]ReadArm{}

// ReadArmFor returns the read arm named arm, if this build has one.
func ReadArmFor(arm string) (ReadArm, bool) {
	run, ok := readArms[arm]
	return run, ok
}

// latencies returns the median and the 99th percentile of d, which it sorts.
func latencies(d []time.Duration) (p50, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0
	}
	slices.Sort(d)
	at := func(q float64) time.Duration { return d[min(int(q*float64(len(d))), len(d)-1)] }
	return at(0.50), at(0.99)
}

// perMGet is n spread over the run's mgets, or 0 when none ran.
func perMGet(n, mgets int64) float64 {
	if mgets == 0 {
		return 0
	}
	return float64(n) / float64(mgets)
}

// PrintRead prints one read phase on one line.
func PrintRead(w io.Writer, label string, r ReadResult) {
	fmt.Fprintf(w, "%-20s %8.0f docs/s  in %7s  p50=%-8s p99=%-8s requests/mget=%-5.2f sent whole=%-6d nodes/mget=%-5.2f "+
		"retries=%-5d via coordinator=%-6d resent=%-4d get tasks/mget=%-5.2f get rejected=%-5d failed=%-4d wrong=%d\n",
		label, r.DocsPerSec, r.Elapsed.Round(time.Millisecond),
		r.P50.Round(10*time.Microsecond), r.P99.Round(10*time.Microsecond),
		perMGet(r.Requests, r.MGets), r.Whole, perMGet(r.Nodes, r.MGets), r.Retries, r.CoordinatorDocs, r.Resent,
		perMGet(r.GetTasks, r.MGets), r.GetRejected, r.Failed, r.Wrong)
}

// SummarizeReads prints the median and the full spread of the read phases,
// never a single run.
func SummarizeReads(w io.Writer, cfg Config, runs []ReadResult) {
	if len(runs) == 0 {
		return
	}
	rate := make([]float64, len(runs))
	p50 := make([]float64, len(runs))
	p99 := make([]float64, len(runs))
	var requests, whole, nodes, tasks, rejected, viaCoordinator, failed, mgets, correct int64
	for i, r := range runs {
		rate[i] = r.DocsPerSec
		p50[i] = float64(r.P50)
		p99[i] = float64(r.P99)
		requests += r.Requests
		whole += r.Whole
		nodes += r.Nodes
		tasks += r.GetTasks
		rejected += r.GetRejected
		viaCoordinator += r.CoordinatorDocs
		failed += r.Failed
		mgets += r.MGets
		if r.Wrong == 0 && r.Failed == 0 {
			correct++
		}
	}
	slices.Sort(rate)
	slices.Sort(p50)
	slices.Sort(p99)

	n := int64(len(runs))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "\narm\tdriver\truns\tdocs/s\tspread\tp50\tp99\trequests/mget\tsent whole\tnodes/mget\tget tasks/mget\tget rejected\tvia coordinator\tfailed\tcorrect\n")
	fmt.Fprintf(tw, "%s\t%s\t%d\t%.0f\t%.0f - %.0f\t%s\t%s\t%.2f\t%d\t%.2f\t%.2f\t%d\t%d\t%d\t%d/%d\n",
		cfg.Arm, cfg.MGetDriver, len(runs), median(rate), rate[0], rate[len(rate)-1],
		time.Duration(median(p50)).Round(10*time.Microsecond), time.Duration(median(p99)).Round(10*time.Microsecond),
		perMGet(requests, mgets), whole/n, perMGet(nodes, mgets), perMGet(tasks, mgets),
		rejected/n, viaCoordinator/n, failed/n, correct, n)
	_ = tw.Flush()

	if correct != n {
		fmt.Fprintf(w, "\nWARNING: %d of %d runs read a document that was missing, out of place, or failed\n",
			n-correct, n)
	}
}
