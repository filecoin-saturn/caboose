package caboose

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/influxdata/tdigest"

	"github.com/serialx/hashring"
)

// TODO Make config vars for tuning
const (
	minAcceptableCorrectnessPct = float64(65)
	maxPoolSize                 = 30

	PLatency         = 0.75
	maxLatencyMs     = float64(200)
	kickOutLatencyMs = float64(1000) // kick out nodes with a P75 TTFB for cache hits > 1 second

	failureDebounce = 1 * time.Minute // helps shield nodes against bursty failures

	tierMain    = "main"
	tierUnknown = "unknown"
)

type perf struct {
	latencyDigest *tdigest.TDigest
	speedDigest   *tdigest.TDigest

	nSuccess int
	nFailure int

	lastBadLatencyAt time.Time
	lastFailureAt    time.Time

	tier string

	// errors
	connFailures  int
	networkErrors int
	responseCodes int
}

// locking is left to the caller
type TieredHashing struct {
	nodes map[string]*perf

	mainSet    *hashring.HashRing
	unknownSet *hashring.HashRing

	removedNodes map[string]struct{}
}

func NewTieredHashing() *TieredHashing {
	return &TieredHashing{
		nodes:        make(map[string]*perf),
		mainSet:      hashring.New(nil),
		unknownSet:   hashring.New(nil),
		removedNodes: make(map[string]struct{}),
	}
}

func (t *TieredHashing) RecordSuccess(node string, rm responseMetrics) {
	if _, ok := t.nodes[node]; !ok {
		return
	}

	perf := t.nodes[node]
	perf.nSuccess++

	if rm.cacheHit {
		perf.speedDigest.Add(rm.speedPerMs, 1)
		// show some lineancy if the node is having a bad time
		if rm.ttfbMS > maxLatencyMs && time.Since(perf.lastBadLatencyAt) < failureDebounce {
			return
		}

		perf.latencyDigest.Add(rm.ttfbMS, 1)
		if rm.ttfbMS > maxLatencyMs {
			perf.lastBadLatencyAt = time.Now()
		}

		if perf.latencyDigest.Count() > 100 && perf.latencyDigest.Quantile(PLatency) > kickOutLatencyMs {
			t.remove(node, perf.tier, "latency")
			return
		}
	}

	if t.isMainSetPolicy(perf) {
		t.mainSet = t.mainSet.AddNode(node)
		t.unknownSet = t.unknownSet.RemoveNode(node)
		perf.tier = tierMain
	} else {
		t.mainSet = t.mainSet.RemoveNode(node)
		t.unknownSet = t.unknownSet.AddNode(node)
		perf.tier = tierUnknown
	}
}

func (t *TieredHashing) remove(node string, tier string, reason string) {
	t.mainSet = t.mainSet.RemoveNode(node)
	t.unknownSet = t.unknownSet.RemoveNode(node)
	delete(t.nodes, node)
	t.removedNodes[node] = struct{}{}
	poolRemovedTotalMetric.WithLabelValues(tier, reason).Inc()
}

func (t *TieredHashing) GetPerf() map[string]*perf {
	return t.nodes
}

func (t *TieredHashing) RecordFailure(node string, rm responseMetrics) {
	if _, ok := t.nodes[node]; !ok {
		return
	}

	perf := t.nodes[node]

	if time.Since(perf.lastFailureAt) < failureDebounce {
		return
	}

	recordFailureFnc := func() {
		perf.nFailure++
		perf.lastFailureAt = time.Now()
	}

	if rm.connFailure {
		recordFailureFnc()
		perf.connFailures++
	} else if rm.networkError {
		recordFailureFnc()
		perf.networkErrors++
	} else if rm.responseCode != http.StatusBadGateway && rm.responseCode != http.StatusGatewayTimeout && rm.responseCode != http.StatusTooManyRequests {
		// TODO Improve this in the next iteration but keep it for now as we are seeing a very high percentage of 502s
		recordFailureFnc()
		perf.responseCodes++
	}

	if !t.isCorrectnessPolicy(perf) {
		t.remove(node, perf.tier, "correctness")

		poolRemovedConnFailureTotalMetric.WithLabelValues(perf.tier).Add(float64(perf.connFailures))
		poolRemovedReadFailureTotalMetric.WithLabelValues(perf.tier).Add(float64(perf.networkErrors))
		poolRemovedNon2xxTotalMetric.WithLabelValues(perf.tier).Add(float64(perf.responseCodes))
	}
}

type poolMetrics struct {
	Unknown int
	Main    int
	Total   int
}

func (t *TieredHashing) GetSize() int {
	unknown := t.unknownSet.Size()
	mainS := t.mainSet.Size()

	return unknown + mainS
}

func (t *TieredHashing) GetPoolMetrics() poolMetrics {
	unknown := t.unknownSet.Size()
	mainS := t.mainSet.Size()

	return poolMetrics{
		Unknown: unknown,
		Main:    mainS,
		Total:   unknown + mainS,
	}
}

func (t *TieredHashing) AddNodes(nodes []string) {
	currSize := t.GetSize()
	poolMembersNotAddedBecauseRemovedMetric.Set(0)

	for _, node := range nodes {
		if len(t.nodes) >= maxPoolSize {
			return
		}
		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}

		// have we kicked this node out for bad correctness ?
		if _, ok := t.removedNodes[node]; ok {
			poolMembersNotAddedBecauseRemovedMetric.Inc()
			continue
		}

		t.nodes[node] = &perf{
			latencyDigest: tdigest.NewWithCompression(1000),
			speedDigest:   tdigest.NewWithCompression(1000),
		}

		if currSize == 0 {
			t.nodes[node].tier = tierMain
			t.mainSet = t.mainSet.AddNode(node)
		} else {
			t.nodes[node].tier = tierUnknown
			t.unknownSet = t.unknownSet.AddNode(node)
		}
	}

}

func (t *TieredHashing) GetNodes(key string, n int) []string {
	// TODO Replace this with mirroring once we have some metrics and correctness info
	// Pick nodes from the unknown set 1 in every 5 times
	fl := rand.Float64()
	var nodes []string
	var ok bool

	if fl <= 0.2 {
		nodes, ok = t.unknownSet.GetNodes(key, n)
		if !ok {
			nodes2, _ := t.mainSet.GetNodes(key, n-len(nodes))
			nodes = append(nodes, nodes2...)
		}
	} else {
		nodes, ok = t.mainSet.GetNodes(key, n)
		if !ok {
			nodes2, _ := t.unknownSet.GetNodes(key, n-len(nodes))
			nodes = append(nodes, nodes2...)
		}
	}

	return nodes
}

func (t *TieredHashing) isMainSetPolicy(perf *perf) bool {
	return perf.latencyDigest.Quantile(PLatency) <= maxLatencyMs
}

func (t *TieredHashing) isCorrectnessPolicy(perf *perf) bool {
	correctnessPct := (float64(perf.nSuccess) / float64(perf.nSuccess+perf.nFailure)) * 100
	return correctnessPct >= minAcceptableCorrectnessPct
}
