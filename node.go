package caboose

import "github.com/zyedidia/generic/queue"

type Node struct {
	URL string

	PredictedLatency     float64
	PredictedThroughput  float64
	PredictedReliability float64

	Samples *queue.Queue[NodeSample]
}

func NewNode(url string) *Node {
	return &Node{
		URL:     url,
		Samples: queue.New[NodeSample](),
	}
}

type NodeSample struct {
	Success    bool
	Latency    float64
	Throughput float64
}

func (n *Node) RecordFailure() {
	n.Samples.Enqueue(NodeSample{Success: false})
}

func (n *Node) RecordSuccess(latency, throughput float64) {
	n.Samples.Enqueue(NodeSample{Success: true, Latency: latency, Throughput: throughput})
}

func (n *Node) Equals(other *Node) bool {
	return n.URL == other.URL
}

func (n *Node) Priority() float64 {
	return n.PredictedReliability / n.PredictedLatency * n.PredictedThroughput
}
