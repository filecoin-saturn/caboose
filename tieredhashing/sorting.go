package tieredhashing

import (
	"sort"

	"github.com/asecurityteam/rolling"
)

func (t *TieredHashing) nodesSortedLatency() ([]nodeWithLatency, []nodeWithTrackedLatency) {
	var nodes []nodeWithLatency

	var nodesWithTrackedLatency []nodeWithTrackedLatency

	for n, perf := range t.nodes {
		perf := perf

		if (perf.LatencyDigest.Reduce(rolling.Percentile(PLatency))) <= trackLifecycleLatency {
			nodesWithTrackedLatency = append(nodesWithTrackedLatency, nodeWithTrackedLatency{
				node:          n,
				nObservations: perf.nLatencyDigest,
			})
		}

		if t.isLatencyWindowFull(perf) {
			nodes = append(nodes, nodeWithLatency{
				node:    n,
				latency: perf.LatencyDigest.Reduce(rolling.Percentile(PLatency)),
			})
		}
	}

	sort.Sort(sortedNodes(nodes))
	return nodes, nodesWithTrackedLatency
}

type nodeWithTrackedLatency struct {
	node          string
	nObservations float64
}

type nodeWithLatency struct {
	node    string
	latency float64
}

type sortedNodes []nodeWithLatency

func (n sortedNodes) Len() int { return len(n) }
func (n sortedNodes) Less(i, j int) bool {
	return n[i].latency <= n[j].latency
}
func (n sortedNodes) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

func (t *TieredHashing) isLatencyWindowFull(perf *NodePerf) bool {
	return perf.nLatencyDigest >= float64(t.cfg.LatencyWindowSize)
}
