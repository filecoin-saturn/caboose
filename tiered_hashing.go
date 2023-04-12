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
	maxPoolSize                 = 50

	PLatency     = 0.75
	maxLatencyMs = float64(200)

	PSpeed         = 0.25
	minSpeedPerSec = float64(1048576 / 2) // 0.5Mib/s

	failureDebounce = 30 * time.Second // helps shield nodes against bursty failures

	tierMain    = "main"
	tierUnknown = "unknown"
)

type perf struct {
	latencyDigest *tdigest.TDigest
	speedDigest   *tdigest.TDigest

	nSuccess       int
	nFailure       int
	correctnessPct float64

	lastFailureAt time.Time

	tier string
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
		t.nodes[node] = &perf{
			latencyDigest: tdigest.NewWithCompression(1000),
			speedDigest:   tdigest.NewWithCompression(1000),
			tier:          tierUnknown,
		}
	}
	perf := t.nodes[node]

	perf.nSuccess++
	perf.correctnessPct = (float64(perf.nSuccess) / float64(perf.nSuccess+perf.nFailure)) * 100
	if rm.cacheHit {
		perf.latencyDigest.Add(rm.ttfbMS, 1)
		perf.speedDigest.Add(rm.speedPerSec, 1)
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

	if rm.isTimeout || rm.connFailure || rm.networkError {
		recordFailureFnc()
	} else if rm.responseCode != http.StatusBadGateway && rm.responseCode != http.StatusGatewayTimeout && rm.responseCode != http.StatusTooManyRequests {
		// TODO Improve this in the next iteration but keep it for now as we are seeing a very high percentage of 502s
		// remove node from main set
		recordFailureFnc()
	}
	perf.correctnessPct = (float64(perf.nSuccess) / float64(perf.nSuccess+perf.nFailure)) * 100

	if !t.isCorrectnessPolicy(perf) {
		t.mainSet = t.mainSet.RemoveNode(node)
		t.unknownSet = t.unknownSet.RemoveNode(node)
		delete(t.nodes, node)
		t.removedNodes[node] = struct{}{}

		poolRemovedCorrectnessTotalMetric.WithLabelValues(perf.tier).Inc()
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

	for _, node := range nodes {
		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}

		// have we kicked this node out for bad correctness ?
		if _, ok := t.removedNodes[node]; ok {
			continue
		}

		t.nodes[node] = &perf{
			latencyDigest: tdigest.NewWithCompression(1000),
			speedDigest:   tdigest.NewWithCompression(1000),
			tier:          tierUnknown,
		}

		if currSize == 0 {
			t.mainSet = t.mainSet.AddNode(node)
		} else {
			t.unknownSet = t.unknownSet.AddNode(node)
		}

		if len(t.nodes) >= maxPoolSize {
			return
		}
	}

	for _, node := range nodes {
		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}
		t.nodes[node] = &perf{
			latencyDigest: tdigest.NewWithCompression(1000),
			speedDigest:   tdigest.NewWithCompression(1000),
			tier:          tierUnknown,
		}
		if currSize == 0 {
			t.mainSet = t.mainSet.AddNode(node)
		} else {
			t.unknownSet = t.unknownSet.AddNode(node)
		}

		if len(t.nodes) >= maxPoolSize {
			return
		}
	}
}

func (t *TieredHashing) GetNodes(key string, n int) []string {
	// TODO Replace this with mirroring once we have some metrics and correctness info
	// Pick nodes from the unknown set 1 in every 5 times
	fl := rand.Float64()
	var nodes []string
	var ok bool

	if fl <= 1/5 {
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
	return perf.latencyDigest.Quantile(PLatency) <= maxLatencyMs &&
		perf.speedDigest.Quantile(PSpeed) >= minSpeedPerSec
}

func (t *TieredHashing) isCorrectnessPolicy(perf *perf) bool {
	return perf.correctnessPct >= minAcceptableCorrectnessPct
}
