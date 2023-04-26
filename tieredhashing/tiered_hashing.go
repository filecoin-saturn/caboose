package tieredhashing

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/asecurityteam/rolling"

	"github.com/patrickmn/go-cache"

	"github.com/serialx/hashring"
)

// TODO Make env vars for tuning
const (
	maxPoolSize     = 200
	maxMainTierSize = 10
	PLatency        = 95

	// main tier has the top `maxMainTierSize` nodes
	tierMain    = "main"
	tierUnknown = "unknown"

	reasonCorrectness = "correctness"
	reasonLatency     = "latency"

	// use rolling windows for latency and correctness calculations
	windowSize = 200

	// ------------------ CORRECTNESS -------------------
	// minimum correctness pct expected from a node over a rolling window over a certain number of observations
	minAcceptableCorrectnessPct = float64(80)

	// helps shield nodes against bursty failures
	failureDebounce = 5 * time.Second
	removalDuration = 24 * time.Hour

	maxAcceptableLatencyPercentile = 90
	maxAcceptableLatency           = 500
)

type NodePerf struct {
	LatencyDigest  *rolling.PointPolicy
	nLatencyDigest float64

	CorrectnessDigest  *rolling.PointPolicy
	nCorrectnessDigest float64

	Tier string

	lastFailureAt time.Time

	// accumulated errors
	connFailures  int
	networkErrors int
	responseCodes int

	// latency
	lastBadLatencyAt time.Time
}

// locking is left to the caller
type TieredHashing struct {
	nodes map[string]*NodePerf

	mainSet    *hashring.HashRing
	unknownSet *hashring.HashRing

	removedNodesTimeCache *cache.Cache

	// config
	cfg TieredHashingConfig

	StartAt  time.Time
	initDone bool
}

func New(opts ...Option) *TieredHashing {
	cfg := &TieredHashingConfig{
		MaxPoolSize:     maxPoolSize,
		FailureDebounce: failureDebounce,
		WindowSize:      windowSize,
		MaxMainTierSize: maxMainTierSize,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	return &TieredHashing{
		nodes:                 make(map[string]*NodePerf),
		mainSet:               hashring.New(nil),
		unknownSet:            hashring.New(nil),
		removedNodesTimeCache: cache.New(removalDuration, 1*time.Minute),
		cfg:                   *cfg,

		StartAt: time.Now(),
	}
}

func (t *TieredHashing) IsInitDone() bool {
	return t.initDone
}

func (t *TieredHashing) RecordSuccess(node string, rm ResponseMetrics) *RemovedNode {
	if _, ok := t.nodes[node]; !ok {
		return nil
	}
	perf := t.nodes[node]
	t.recordCorrectness(perf, true)

	// we only consider cache hit latencies for now
	if !rm.CacheHit {
		return nil
	}

	// show some lineancy if the node is having a bad time
	if rm.TTFBMs > maxAcceptableLatency && time.Since(perf.lastBadLatencyAt) < t.cfg.FailureDebounce {
		return nil
	}
	// record the latency and update the last bad latency record time if needed
	perf.LatencyDigest.Append(rm.TTFBMs)
	perf.nLatencyDigest++
	if rm.TTFBMs > maxAcceptableLatency {
		perf.lastBadLatencyAt = time.Now()
	}

	if t.isLatencyWindowFull(perf) && perf.LatencyDigest.Reduce(rolling.Percentile(maxAcceptableLatencyPercentile)) > maxAcceptableLatency {
		mc, uc := t.removeFailedNode(node)
		return &RemovedNode{
			Node:                node,
			Tier:                perf.Tier,
			Reason:              reasonLatency,
			MainToUnknownChange: mc,
			UnknownToMainChange: uc,
		}
	}

	return nil
}

type RemovedNode struct {
	Node                string
	Tier                string
	Reason              string
	ConnErrors          int
	NetworkErrors       int
	ResponseCodes       int
	MainToUnknownChange int
	UnknownToMainChange int
}

func (t *TieredHashing) DoRefresh() bool {
	return t.GetPoolMetrics().Total <= (t.cfg.MaxPoolSize / 10)
}

func (t *TieredHashing) RecordFailure(node string, rm ResponseMetrics) *RemovedNode {
	if _, ok := t.nodes[node]; !ok {
		return nil
	}

	perf := t.nodes[node]
	if time.Since(perf.lastFailureAt) < t.cfg.FailureDebounce {
		return nil
	}

	recordFailureFnc := func() {
		t.recordCorrectness(perf, false)
		perf.lastFailureAt = time.Now()
	}

	if rm.ConnFailure {
		recordFailureFnc()
		perf.connFailures++
	} else if rm.NetworkError {
		recordFailureFnc()
		perf.networkErrors++
	} else if rm.ResponseCode != http.StatusBadGateway && rm.ResponseCode != http.StatusGatewayTimeout && rm.ResponseCode != http.StatusTooManyRequests {
		// TODO Improve this in the next iteration but keep it for now as we are seeing a very high percentage of 502s
		recordFailureFnc()
		perf.responseCodes++
	}

	if _, ok := t.isCorrectnessPolicyEligible(perf); !ok {
		mc, uc := t.removeFailedNode(node)
		return &RemovedNode{
			Node:                node,
			Tier:                perf.Tier,
			Reason:              reasonCorrectness,
			ConnErrors:          perf.connFailures,
			NetworkErrors:       perf.networkErrors,
			ResponseCodes:       perf.responseCodes,
			MainToUnknownChange: mc,
			UnknownToMainChange: uc,
		}
	}

	return nil
}

type PoolMetrics struct {
	Unknown int
	Main    int
	Total   int
}

func (t *TieredHashing) GetPoolMetrics() PoolMetrics {
	unknown := t.unknownSet.Size()
	mainS := t.mainSet.Size()

	return PoolMetrics{
		Unknown: unknown,
		Main:    mainS,
		Total:   unknown + mainS,
	}
}

func (t *TieredHashing) GetNodes(key string, n int) []string {
	// TODO Replace this with mirroring once we have some metrics and correctness info
	// Pick nodes from the unknown tier 1 in every 10 times
	var nodes []string
	var ok bool

	if !t.cfg.AlwaysMainFirst && rand.Float64() <= 0.1 {
		if t.unknownSet.Size() != 0 {
			nodes, ok = t.unknownSet.GetNodes(key, t.unknownPossible(n))
			if !ok {
				return nil
			}
			if len(nodes) == n {
				return nodes
			}
		}

		nodes2, _ := t.mainSet.GetNodes(key, t.mainPossible(n-len(nodes)))
		nodes = append(nodes, nodes2...)
	} else {
		if t.mainSet.Size() != 0 {
			nodes, ok = t.mainSet.GetNodes(key, t.mainPossible(n))
			if !ok {
				return nil
			}
			if len(nodes) == n {
				return nodes
			}
		}

		nodes2, _ := t.unknownSet.GetNodes(key, t.unknownPossible(n-len(nodes)))
		nodes = append(nodes, nodes2...)
	}

	return nodes
}

func (t *TieredHashing) unknownPossible(n int) int {
	if n > t.unknownSet.Size() {
		return t.unknownSet.Size()
	} else {
		return n
	}
}

func (t *TieredHashing) mainPossible(n int) int {
	if n > t.mainSet.Size() {
		return t.mainSet.Size()
	} else {
		return n
	}
}

func (t *TieredHashing) GetPerf() map[string]*NodePerf {
	return t.nodes
}

func (t *TieredHashing) AddOrchestratorNodes(nodes []string) (added, alreadyRemoved int) {
	added = 0
	alreadyRemoved = 0

	for _, node := range nodes {
		// TODO Add nodes that are closer than the ones we have even if the pool is full
		if len(t.nodes) >= t.cfg.MaxPoolSize {
			return
		}

		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}

		// have we kicked this node out for bad correctness or latency ?
		if _, ok := t.removedNodesTimeCache.Get(node); ok {
			alreadyRemoved++
			continue
		}

		added++
		t.nodes[node] = &NodePerf{
			LatencyDigest:     rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.WindowSize))),
			CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.WindowSize))),
			Tier:              tierUnknown,
		}
		t.unknownSet = t.unknownSet.AddNode(node)
	}

	return
}

func (t *TieredHashing) UpdateMainTierWithTopN() (mainToUnknown, unknownToMain int) {
	// sort all nodes by P95 and pick the top N as main tier nodes
	nodes := t.nodesSortedLatency()

	if !t.initDone {
		if len(nodes) < t.cfg.MaxMainTierSize {
			return
		}
		t.initDone = true
	}
	if len(nodes) < t.cfg.MaxMainTierSize {
		return
	}

	mainTier := nodes[:t.cfg.MaxMainTierSize]
	unknownTier := nodes[t.cfg.MaxMainTierSize:]

	for _, nodeL := range mainTier {
		if t.nodes[nodeL.node].Tier == tierUnknown {
			unknownToMain++
			n := nodeL.node
			t.mainSet = t.mainSet.AddNode(n)
			t.unknownSet = t.unknownSet.RemoveNode(n)
			t.nodes[n].Tier = tierMain
		}
	}

	for _, nodeL := range unknownTier {
		if t.nodes[nodeL.node].Tier == tierMain {
			mainToUnknown++
			n := nodeL.node
			t.unknownSet = t.unknownSet.AddNode(n)
			t.mainSet = t.mainSet.RemoveNode(n)
			t.nodes[n].Tier = tierUnknown
		}
	}

	return
}

func (t *TieredHashing) isCorrectnessPolicyEligible(perf *NodePerf) (float64, bool) {
	// we don't have enough observations yet
	if perf.nCorrectnessDigest < float64(t.cfg.WindowSize) {
		return 0, true
	} else {
		perf.nCorrectnessDigest = float64(t.cfg.WindowSize)
	}

	totalSuccess := perf.CorrectnessDigest.Reduce(func(w rolling.Window) float64 {
		var result float64
		for _, bucket := range w {
			for _, p := range bucket {
				if p == 1 {
					result++
				}
			}
		}
		return result
	})

	// should satisfy a certain minimum percentage
	pct := totalSuccess / perf.nCorrectnessDigest * 100

	return pct, pct >= minAcceptableCorrectnessPct
}

func (t *TieredHashing) removeFailedNode(node string) (mc, uc int) {
	perf := t.nodes[node]
	t.mainSet = t.mainSet.RemoveNode(node)
	t.unknownSet = t.unknownSet.RemoveNode(node)
	delete(t.nodes, node)
	t.removedNodesTimeCache.Set(node, struct{}{}, cache.DefaultExpiration)

	if perf.Tier == tierMain {
		// if we've removed a main set node we should replace it
		mc, uc = t.UpdateMainTierWithTopN()
	}
	return
}

func (t *TieredHashing) recordCorrectness(perf *NodePerf, success bool) {
	if success {
		perf.CorrectnessDigest.Append(1)
	} else {
		perf.CorrectnessDigest.Append(0)
	}
	perf.nCorrectnessDigest++
	if perf.nCorrectnessDigest > float64(t.cfg.WindowSize) {
		perf.nCorrectnessDigest = float64(t.cfg.WindowSize)
	}
}
