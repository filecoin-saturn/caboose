package tieredhashing

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/influxdata/tdigest"

	"github.com/serialx/hashring"
)

// TODO Make config vars for tuning
const (
	maxPoolSize        = 50
	maxPoolSizeIfEmpty = 300

	PLatency                   = 0.75
	maxLatencyForMainSetMillis = float64(300)
	tierMain                   = "main"
	tierUnknown                = "unknown"

	// ------------------ CORRECTNESS -------------------
	// minimum fetch requests to a node before we make a call on it's correctness
	minObservationsForCorrectness = 120
	// minimum correctness pct expected from a node over a rolling window over a certain number of observations
	minAcceptableCorrectnessPct = float64(70)
	// helps shield nodes against bursty failures
	failureDebounce   = 10 * time.Second
	removalDuration   = 48 * time.Hour
	maxWindowDuration = 30 * time.Minute
)

type NodePerf struct {
	tier string

	LatencyDigest *tdigest.TDigest
	SpeedDigest   *tdigest.TDigest

	// correctness -> reset at window
	nSuccess               int
	nFailure               int
	correctnessWindowStart time.Time
	lastFailureCountAt     time.Time

	// accumulated errors
	connFailures  int
	networkErrors int
	responseCodes int

	// latency -> reset at window
	lastBadLatencyAt time.Time
	tierWindowStart  time.Time
}

// locking is left to the caller
type TieredHashing struct {
	nodes map[string]*NodePerf

	mainSet    *hashring.HashRing
	unknownSet *hashring.HashRing

	removedNodesTimeCache *cache.Cache

	// config
	cfg TieredHashingConfig
}

func New(opts ...Option) *TieredHashing {
	cfg := &TieredHashingConfig{
		MaxPoolSizeNonEmpty: maxPoolSize,
		MaxPoolSizeEmpty:    maxPoolSizeIfEmpty,
		FailureDebounce:     failureDebounce,
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
	}
}

// TODO Main set policy should incorporate some form of correctness
// if the metrics show that main tier peers are getting kicked out because of correctness
func (t *TieredHashing) RecordSuccess(node string, rm ResponseMetrics) {
	if _, ok := t.nodes[node]; !ok {
		return
	}

	perf := t.nodes[node]
	perf.nSuccess++

	if !rm.CacheHit {
		return
	}

	perf.SpeedDigest.Add(rm.SpeedPerMs, 1)
	// show some lineancy if the node is having a bad time
	if rm.TTFBMs > maxLatencyForMainSetMillis && time.Since(perf.lastBadLatencyAt) < t.cfg.FailureDebounce {
		return
	}

	// record the latency and update the last bad latency record time if needed
	perf.LatencyDigest.Add(rm.TTFBMs, 1)
	if rm.TTFBMs > maxLatencyForMainSetMillis {
		perf.lastBadLatencyAt = time.Now()
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

	// refresh latency window to only consider "recent" observations
	if time.Since(perf.tierWindowStart) > maxWindowDuration {
		perf.tierWindowStart = time.Now()
		perf.LatencyDigest = tdigest.NewWithCompression(1000)
		perf.SpeedDigest = tdigest.NewWithCompression(1000)
	}

	return
}

func (t *TieredHashing) GetPerf() map[string]*NodePerf {
	return t.nodes
}

type FailureRemoved struct {
	Tier          string
	Reason        string
	ConnErrors    int
	NetworkErrors int
	ResponseCodes int
}

func (t *TieredHashing) DoRefresh() bool {
	return t.GetPoolMetrics().Total <= (t.cfg.MaxPoolSizeNonEmpty / 2)
}

func (t *TieredHashing) RecordFailure(node string, rm ResponseMetrics) *FailureRemoved {
	if _, ok := t.nodes[node]; !ok {
		return nil
	}

	perf := t.nodes[node]
	if time.Since(perf.lastFailureCountAt) < t.cfg.FailureDebounce {
		return nil
	}

	recordFailureFnc := func() {
		perf.nFailure++
		perf.lastFailureCountAt = time.Now()
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

	if !t.isCorrectnessPolicyEligible(perf) {
		t.removeFailed(node)
		return &FailureRemoved{
			Tier:          perf.tier,
			Reason:        "correctness",
			ConnErrors:    perf.connFailures,
			NetworkErrors: perf.networkErrors,
			ResponseCodes: perf.responseCodes,
		}
	}

	// look at correctness over a given window size if we've already seen enough observations
	if time.Since(perf.correctnessWindowStart) > maxWindowDuration && (perf.nFailure+perf.nSuccess >= minObservationsForCorrectness) {
		perf.nSuccess = 0
		perf.nFailure = 0
		perf.correctnessWindowStart = time.Now()
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
	// Pick nodes from the unknown set 1 in every 5 times
	var nodes []string
	var ok bool

	if !t.cfg.AlwaysMainFirst && rand.Float64() <= 0.2 {
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

func (t *TieredHashing) AddOrchestratorNodes(nodes []string) (added, removed, alreadyRemoved int) {
	removed = t.removeNodesNotInOrchestrator(nodes)
	added = 0
	alreadyRemoved = 0
	isEmpty := t.GetPoolMetrics().Total == 0

	for _, node := range nodes {
		if !isEmpty && len(t.nodes) >= t.cfg.MaxPoolSizeNonEmpty {
			return
		} else if isEmpty && len(t.nodes) >= t.cfg.MaxPoolSizeEmpty { // Fill more nodes if pool is empty
			return
		}

		// do we already have this node ?
		if _, ok := t.nodes[node]; ok {
			continue
		}

		// have we kicked this node out for bad correctness ?
		if _, ok := t.removedNodesTimeCache.Get(node); ok {
			alreadyRemoved++
			continue
		}

		added++
		t.nodes[node] = &NodePerf{
			LatencyDigest:          tdigest.NewWithCompression(1000),
			SpeedDigest:            tdigest.NewWithCompression(1000),
			correctnessWindowStart: time.Now(),
			tierWindowStart:        time.Now(),
		}
		if isEmpty {
			t.nodes[node].tier = tierMain
			t.mainSet = t.mainSet.AddNode(node)
		} else {
			t.nodes[node].tier = tierUnknown
			t.unknownSet = t.unknownSet.AddNode(node)
		}
	}

	return
}

func (t *TieredHashing) removeNodesNotInOrchestrator(nodes []string) int {
	count := 0
	orchNode := make(map[string]struct{})
	for _, node := range nodes {
		orchNode[node] = struct{}{}
	}

	// If we have a node that the orchestrator does not have, remove it
	var toRemove []string
	for n, _ := range t.nodes {
		if _, ok := orchNode[n]; !ok {
			count++
			toRemove = append(toRemove, n)
		}
	}

	for _, n := range toRemove {
		t.mainSet = t.mainSet.RemoveNode(n)
		t.unknownSet = t.unknownSet.RemoveNode(n)
		delete(t.nodes, n)
	}

	return count
}

func (t *TieredHashing) isMainSetPolicy(perf *NodePerf) bool {
	return perf.LatencyDigest.Count() > 0 && perf.LatencyDigest.Quantile(PLatency) <= maxLatencyForMainSetMillis
}

func (t *TieredHashing) isCorrectnessPolicyEligible(perf *NodePerf) bool {
	// no failure seen till now
	if perf.nFailure == 0 {
		return true
	}

	total := perf.nSuccess + perf.nFailure
	// we don't have enough observations yet
	if total < minObservationsForCorrectness {
		return true
	}

	// should satisfy a certain minimum percentage
	correctnessPct := (float64(perf.nSuccess) / float64(total)) * 100
	return correctnessPct >= minAcceptableCorrectnessPct
}

func (t *TieredHashing) removeFailed(node string) {
	t.mainSet = t.mainSet.RemoveNode(node)
	t.unknownSet = t.unknownSet.RemoveNode(node)
	delete(t.nodes, node)
	t.removedNodesTimeCache.Set(node, struct{}{}, cache.DefaultExpiration)
}
