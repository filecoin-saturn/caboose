package caboose

import (
	"math"
	"sync"
	"time"

	"github.com/filecoin-saturn/caboose/internal/state"
	"github.com/zyedidia/generic/queue"
)

const (
	sampleWindow     = 100.0
	defaultLatencyMS = 300.0
)

type Node struct {
	URL           string
	ComplianceCid string
	Core          bool

	PredictedLatency     float64
	PredictedThroughput  float64
	PredictedReliability float64

	Samples   *queue.Queue[NodeSample]
	successes int
	lk        sync.RWMutex
}

func NewNode(info state.NodeInfo) *Node {
	return &Node{
		URL:           info.IP,
		ComplianceCid: info.ComplianceCid,
		Core:          info.Core,
		Samples:       queue.New[NodeSample](),
	}
}

type NodeSample struct {
	Start, End time.Time
	Success    bool
	// recorded in milliseconds
	Latency float64
	Size    float64
}

func (n *Node) RecordFailure() {
	n.lk.Lock()
	defer n.lk.Unlock()
	n.Samples.Enqueue(NodeSample{Success: false})
	n.update()
}

func (n *Node) RecordSuccess(start time.Time, latency, size float64) {
	n.lk.Lock()
	defer n.lk.Unlock()
	n.Samples.Enqueue(NodeSample{Start: start, End: time.Now(), Success: true, Latency: latency, Size: size})
	n.successes += 1
	n.update()
}

func (n *Node) update() {
	s := n.Samples.Peek()
	successN := 0
	if s.Success {
		successN = 1
	}
	decayFactor := (sampleWindow - 1) / sampleWindow
	n.PredictedReliability = n.PredictedReliability*decayFactor + float64(successN)/sampleWindow

	to := time.Time{}
	totalBytesPerMS := 0.0
	latency := defaultLatencyMS

	decay := 1 * math.Pow(decayFactor, float64(n.successes))
	// We want to treat this as an expontential decay of the samples, e.g.
	// latest * 1/n + (2nd latest * 1/n + ...) * (n-1/n)
	// This basic decay-weighting is enough for latency measurements
	// with the caveat that we have 'missing' (non-successful) elements that shouldn't affect the denominator.
	// For throughput, when there are subsequent retrievals that happen in the same time period, we want to
	// count that as a term like `(latest + 2nd latest) * 2/n` - the observed throughput at that point is higher
	// than the individual measurements.
	n.Samples.Each(func(t NodeSample) {
		if !t.Success {
			return
		}
		decay /= decayFactor

		totalBytesPerMS += t.Size / float64(t.End.Sub(t.Start).Milliseconds())

		// fix up in case
		if t.Start.Before(to) {
			// overlap
			fractionOverlap := float64(to.Sub(t.Start)) / float64(t.End.Sub(t.Start))
			totalBytesPerMS *= decay / fractionOverlap
		} else {
			totalBytesPerMS *= decay
		}
		to = t.End
	})
	n.PredictedLatency = latency
	n.PredictedThroughput = totalBytesPerMS

	if n.Samples.Len() > sampleWindow {
		old := n.Samples.Dequeue()
		if old.Success {
			n.successes -= 1
		}
	}
}

func (n *Node) Equals(other *Node) bool {
	return n.URL == other.URL
}

func (n *Node) Priority() float64 {
	n.lk.RLock()
	defer n.lk.RUnlock()
	latency := n.PredictedLatency
	if latency == 0 {
		latency = defaultLatencyMS
	}

	return n.PredictedReliability / latency * n.PredictedThroughput
}

func (n *Node) Rate() float64 {
	n.lk.RLock()
	defer n.lk.RUnlock()

	len := n.Samples.Len()
	if len == 0 {
		return 0
	}
	last := n.Samples.Peek()
	return float64(len) / float64(time.Since(last.Start))
}

func (n *Node) String() string {
	return n.URL
}
