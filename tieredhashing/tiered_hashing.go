package tieredhashing

import (
	"net/http"
	"time"

	"github.com/asecurityteam/rolling"

	"github.com/patrickmn/go-cache"

	"github.com/serialx/hashring"
)

// TODO Make env vars for tuning
const (
	maxPoolSize     = 50
	maxMainTierSize = 10
	PLatency        = 90

	// main tier has the top `maxMainTierSize` nodes
	TierMain    = Tier("main")
	TierUnknown = Tier("unknown")

	reasonCorrectness = "correctness"

	// use rolling windows for latency and correctness calculations
	latencyWindowSize     = 1000
	correctnessWindowSize = 1000

	// ------------------ CORRECTNESS -------------------
	// minimum correctness pct expected from a node over a rolling window over a certain number of observations
	minAcceptableCorrectnessPct = float64(70)

	// helps shield nodes against bursty failures
	failureDebounce = 2 * time.Second
	removalDuration = 2 * time.Hour
)

type Tier string

type NodePerf struct {
	LatencyDigest  *rolling.PointPolicy
	NLatencyDigest float64

	CorrectnessDigest  *rolling.PointPolicy
	NCorrectnessDigest float64

	Tier

	lastFailureAt time.Time

	// accumulated errors
	connFailures  int
	networkErrors int
	responseCodes int
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
		MaxPoolSize:           maxPoolSize,
		FailureDebounce:       failureDebounce,
		LatencyWindowSize:     latencyWindowSize,
		CorrectnessWindowSize: correctnessWindowSize,
		CorrectnessPct:        minAcceptableCorrectnessPct,
		MaxMainTierSize:       maxMainTierSize,
		NoRemove:              false,
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

func (t *TieredHashing) RecordSuccess(node string, rm ResponseMetrics) {
	if _, ok := t.nodes[node]; !ok {
		return
	}
	perf := t.nodes[node]
	t.recordCorrectness(perf, true)
	// record the latency and update the last bad latency record time if needed
	perf.LatencyDigest.Append(rm.TTFBMs)
	perf.NLatencyDigest++
}

type RemovedNode struct {
	Node string
	Tier
	Reason              string
	ConnErrors          int
	NetworkErrors       int
	ResponseCodes       int
	MainToUnknownChange int
	UnknownToMainChange int
	ResponseCodesMap    map[int]int
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
	} else if rm.ResponseCode != http.StatusBadGateway && rm.ResponseCode != http.StatusGatewayTimeout &&
		rm.ResponseCode != http.StatusTooManyRequests && rm.ResponseCode != http.StatusForbidden {
		// TODO Improve this in the next iteration but keep it for now as we are seeing a very high percentage of 502s
		recordFailureFnc()
		perf.responseCodes++
	}

	if !t.cfg.NoRemove {
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

func (t *TieredHashing) GetNodes(from Tier, key string, n int) []string {
	var nodes []string
	var ok bool

	if from == TierUnknown {
		nodes, ok = t.unknownSet.GetNodes(key, t.unknownPossible(n))
		if !ok {
			return nil
		}
	} else if from == TierMain {
		nodes, ok = t.mainSet.GetNodes(key, t.mainPossible(n))
		if !ok {
			return nil
		}
	}

	return nodes
}

func (t *TieredHashing) NodeTier(node string) Tier {
	for k, n := range t.nodes {
		if k == node {
			return n.Tier
		}
	}
	return TierUnknown
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

func (t *TieredHashing) AddOrchestratorNodes(nodes []string) (added, alreadyRemoved, removedAndAddedBack int) {
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
			LatencyDigest:     rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.LatencyWindowSize))),
			CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.CorrectnessWindowSize))),
			Tier:              TierUnknown,
		}
		t.unknownSet = t.unknownSet.AddNode(node)
	}

	// Avoid Pool starvation -> if we still don't have enough nodes, add the ones we have already removed
	// we ensure we iterate in descending order of node closeness
	for _, node := range nodes {
		if len(t.nodes) >= t.cfg.MaxPoolSize {
			return
		}

		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}

		if _, ok := t.removedNodesTimeCache.Get(node); !ok {
			continue
		}

		added++
		removedAndAddedBack++
		t.nodes[node] = &NodePerf{
			LatencyDigest:     rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.LatencyWindowSize))),
			CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(t.cfg.CorrectnessWindowSize))),
			Tier:              TierUnknown,
		}
		t.unknownSet = t.unknownSet.AddNode(node)
		t.removedNodesTimeCache.Delete(node)
	}

	return
}

func (t *TieredHashing) UpdateMainTierWithTopN() (mainToUnknown, unknownToMain int) {
	/*// sort all nodes by P95 and pick the top N as main tier nodes
	nodes := t.nodesSortedLatency()
	if len(nodes) == 0 {
		return
	}

	// bulk update initially so we don't end up dosing the nodes
	if !t.initDone {
		if len(nodes) < t.cfg.MaxMainTierSize {
			return
		}
		t.initDone = true
	}

	// Main Tier should have MIN(number of eligible nodes, max main tier size) nodes
	n := t.cfg.MaxMainTierSize
	if len(nodes) < t.cfg.MaxMainTierSize {
		n = len(nodes)
	}

	mainTier := nodes[:n]
	unknownTier := nodes[n:]

	for _, nodeL := range mainTier {
		if t.nodes[nodeL.node].Tier == TierUnknown {
			unknownToMain++
			n := nodeL.node
			t.mainSet = t.mainSet.AddNode(n)
			t.unknownSet = t.unknownSet.RemoveNode(n)
			t.nodes[n].Tier = TierMain
		}
	}

	for _, nodeL := range unknownTier {
		if t.nodes[nodeL.node].Tier == TierMain {
			mainToUnknown++
			n := nodeL.node
			t.unknownSet = t.unknownSet.AddNode(n)
			t.mainSet = t.mainSet.RemoveNode(n)
			t.nodes[n].Tier = TierUnknown
		}
	}*/

	return
}

func (t *TieredHashing) isCorrectnessPolicyEligible(perf *NodePerf) (float64, bool) {
	// we don't have enough observations yet
	if perf.NCorrectnessDigest < float64(t.cfg.CorrectnessWindowSize) {
		return 0, true
	} else {
		perf.NCorrectnessDigest = float64(t.cfg.CorrectnessWindowSize)
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
	pct := totalSuccess / perf.NCorrectnessDigest * 100

	return pct, pct >= t.cfg.CorrectnessPct
}

func (t *TieredHashing) removeFailedNode(node string) (mc, uc int) {
	perf := t.nodes[node]
	t.mainSet = t.mainSet.RemoveNode(node)
	t.unknownSet = t.unknownSet.RemoveNode(node)
	delete(t.nodes, node)
	t.removedNodesTimeCache.Set(node, struct{}{}, cache.DefaultExpiration)

	if perf.Tier == TierMain {
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
	perf.NCorrectnessDigest++
	if perf.NCorrectnessDigest > float64(t.cfg.CorrectnessWindowSize) {
		perf.NCorrectnessDigest = float64(t.cfg.CorrectnessWindowSize)
	}
}
