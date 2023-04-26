package tieredhashing

import (
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/asecurityteam/rolling"

	"github.com/stretchr/testify/require"
)

func TestRecordSuccess(t *testing.T) {
	window := 3

	th := NewTieredHashingHarness(WithWindowSize(window), WithFailureDebounce(0))
	// main node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	// cache miss -> record success but latency no-op
	require.Nil(t, th.h.RecordSuccess(unknownNode, ResponseMetrics{}))
	require.EqualValues(t, 1, th.nSuccess(unknownNode))
	th.assertSize(t, 0, 1)
	require.EqualValues(t, 0, th.h.nodes[unknownNode].nLatencyDigest)

	// cache miss -> record success but latency no-op
	require.Nil(t, th.h.RecordSuccess(unknownNode, ResponseMetrics{}))
	require.EqualValues(t, 2, th.nSuccess(unknownNode))
	th.assertSize(t, 0, 1)
	require.EqualValues(t, 0, th.h.nodes[unknownNode].nLatencyDigest)

	// cache hits
	require.Nil(t, th.recordCacheHitAndAssertSet(t, unknownNode, 200, 0, 1, tierUnknown))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].nLatencyDigest)
	require.EqualValues(t, 200, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	require.Nil(t, th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 150}))
	require.EqualValues(t, 2, th.h.nodes[unknownNode].nLatencyDigest)
	require.EqualValues(t, 350, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	require.Nil(t, th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 50}))
	require.EqualValues(t, 3, th.h.nodes[unknownNode].nLatencyDigest)
	require.EqualValues(t, 400, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	// windowing
	require.Nil(t, th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 60}))
	require.EqualValues(t, 4, th.h.nodes[unknownNode].nLatencyDigest)
	require.EqualValues(t, 260, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	// node gets removed for unacceptable latency
	rm := th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 600})
	require.NotNil(t, rm)
	require.EqualValues(t, tierUnknown, rm.Tier)
	require.EqualValues(t, reasonLatency, rm.Reason)
	th.assertSize(t, 0, 0)

	// node does not get removed for unacceptable latency if not enough observations
	unknownNode = th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)
	rm = th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 6000})
	require.Nil(t, rm)
	th.assertSize(t, 0, 1)

	rm = th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 6000})
	require.Nil(t, rm)

	// gets removed as enough observations
	rm = th.h.RecordSuccess(unknownNode, ResponseMetrics{CacheHit: true, TTFBMs: 6000})
	require.NotNil(t, rm)
	th.assertSize(t, 0, 0)
}

func (th *TieredHashingHarness) recordCacheHitAndAssertSet(t *testing.T, node string, ttfbMS float64, mc, uc int, tier string) *RemovedNode {
	prevSuccess := th.nSuccess(node)
	rm := th.h.RecordSuccess(node, ResponseMetrics{CacheHit: true, TTFBMs: ttfbMS})
	require.EqualValues(t, prevSuccess+1, th.nSuccess(node))
	th.assertSize(t, mc, uc)
	require.EqualValues(t, th.h.nodes[node].Tier, tier)
	return rm
}

func TestRecordFailure(t *testing.T) {
	window := 3

	th := NewTieredHashingHarness(WithWindowSize(window), WithFailureDebounce(0))
	// main node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	// 502 status code no change
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ResponseCode: http.StatusBadGateway}))
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ResponseCode: http.StatusGatewayTimeout}))
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ConnFailure: true}))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].connFailures)

	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true}))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].networkErrors)

	// node is evicted as we have enough observations
	rm := th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true})
	require.NotNil(t, rm)
	require.EqualValues(t, tierUnknown, rm.Tier)
	require.EqualValues(t, unknownNode, rm.Node)
}

func TestNodeEvictionWithWindowing(t *testing.T) {
	window := 4

	th := NewTieredHashingHarness(WithWindowSize(window), WithFailureDebounce(0))
	// main node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	th.h.mainSet.AddNode(unknownNode)
	th.h.unknownSet.RemoveNode(unknownNode)
	th.h.nodes[unknownNode].Tier = tierMain

	// record success
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})

	// evicted as pct < 80 because of windowing
	rm := th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true})
	require.NotNil(t, rm)
	require.EqualValues(t, tierMain, rm.Tier)
	require.EqualValues(t, unknownNode, rm.Node)
}

func TestGetNodes(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	assertCountF := func(t *testing.T, resp []string, mc, uc int) {
		var countMain int
		var countUnknown int
		for _, n := range resp {
			if th.h.nodes[n].Tier == tierMain {
				countMain++
			} else {
				countUnknown++
			}
		}
		require.EqualValues(t, mc, countMain)
		require.EqualValues(t, uc, countUnknown)
	}

	// empty
	nds := th.h.GetNodes("test", 1)
	require.Empty(t, nds)

	// has 3 unknown, 0 main
	unknownNodes := th.genAndAddAll(t, 3)
	th.assertSize(t, 0, 3)
	resp := th.h.GetNodes("test", 100)
	require.Len(t, resp, 3)
	assertCountF(t, resp, 0, 3)

	// has 2 main, 3 unknown
	mainNodes := th.genAndAddAll(t, 2)
	for _, n := range mainNodes {
		th.h.nodes[n].Tier = tierMain
		th.h.mainSet = th.h.mainSet.AddNode(n)
		th.h.unknownSet = th.h.unknownSet.RemoveNode(n)
	}

	th.assertSize(t, 2, 3)
	resp = th.h.GetNodes("test", 100)
	require.Len(t, resp, 5)
	assertCountF(t, resp, 2, 3)

	assertGetAndCountF := func(t *testing.T, mainS int, unknownS int, n int, len int, mc, uc int) {
		th.assertSize(t, mainS, unknownS)
		resp = th.h.GetNodes("test", n)
		require.Len(t, resp, len)
		assertCountF(t, resp, mc, uc)
	}

	// has both main
	assertGetAndCountF(t, 2, 3, 2, 2, 2, 0)

	th.h.removeFailedNode(mainNodes[0])
	assertGetAndCountF(t, 1, 3, 10, 4, 1, 3)

	// has 1 main, 1 unknown
	assertGetAndCountF(t, 1, 3, 2, 2, 1, 1)

	// has 1 main, 0 unknown
	assertGetAndCountF(t, 1, 3, 1, 1, 1, 0)

	// has 1 main, 2 unknown
	th.h.removeFailedNode(unknownNodes[0])
	assertGetAndCountF(t, 1, 2, 10, 3, 1, 2)

	// has 0 main, 1 unknown
	th.h.removeFailedNode(mainNodes[1])
	assertGetAndCountF(t, 0, 2, 1, 1, 0, 1)

	// has 0 main, 0 unknown
	th.h.removeFailedNode(unknownNodes[1])
	th.h.removeFailedNode(unknownNodes[2])
	assertGetAndCountF(t, 0, 0, 1, 0, 0, 0)
}

func TestConsistentHashing(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	th.genAndAddAll(t, 10)
	th.assertSize(t, 0, 10)
	resp1 := th.h.GetNodes("test", 3)
	require.Len(t, resp1, 3)

	resp2 := th.h.GetNodes("test", 2)
	require.Len(t, resp2, 2)

	require.EqualValues(t, resp1[:2], resp2[:2])
}

func TestRecordCorrectness(t *testing.T) {
	window := 3
	th := NewTieredHashingHarness(WithWindowSize(window))
	perf := &NodePerf{
		CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
	}
	th.h.recordCorrectness(perf, true)
	require.EqualValues(t, 1, perf.nCorrectnessDigest)
	require.EqualValues(t, 1, perf.CorrectnessDigest.Reduce(rolling.Sum))

	th.h.recordCorrectness(perf, true)
	require.EqualValues(t, 2, perf.CorrectnessDigest.Reduce(rolling.Sum))
	require.EqualValues(t, 2, perf.nCorrectnessDigest)

	th.h.recordCorrectness(perf, false)
	require.EqualValues(t, 3, perf.nCorrectnessDigest)
	require.EqualValues(t, 2, perf.CorrectnessDigest.Reduce(rolling.Sum))

	th.h.recordCorrectness(perf, false)
	require.EqualValues(t, 3, perf.nCorrectnessDigest)
	require.EqualValues(t, 1, perf.CorrectnessDigest.Reduce(rolling.Sum))
}

func (th *TieredHashingHarness) updateTiersAndAsert(t *testing.T, mcs, ucs, mains, unknowns int, isInitDone bool, mainNodes []string) {
	mc, uc := th.h.UpdateMainTierWithTopN()
	require.EqualValues(t, mcs, mc)
	require.EqualValues(t, ucs, uc)
	th.assertSize(t, mains, unknowns)
	require.EqualValues(t, isInitDone, th.h.IsInitDone())

	var mnodes []string
	for n, perf := range th.h.nodes {
		perf := perf
		if perf.Tier == tierMain {
			mnodes = append(mnodes, n)
		}
	}

	sort.Slice(mainNodes, func(i, j int) bool {
		return mainNodes[i] < mainNodes[j]
	})

	sort.Slice(mnodes, func(i, j int) bool {
		return mnodes[i] < mnodes[j]
	})

	require.EqualValues(t, mainNodes, mnodes)
}

func TestUpdateMainTierWithTopN(t *testing.T) {
	windowSize := 2
	th := NewTieredHashingHarness(WithWindowSize(windowSize), WithMaxMainTierSize(2))

	mc, uc := th.h.UpdateMainTierWithTopN()
	require.Zero(t, mc)
	require.Zero(t, uc)

	// main node
	nodes := th.genAndAddAll(t, 5)
	th.assertSize(t, 0, 5)

	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// Record 1 observation for a node -> no change
	th.h.RecordSuccess(nodes[0], ResponseMetrics{CacheHit: true, TTFBMs: 100})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// record 1 more observation for the same node -> no change as not enough nodes for bulk update
	th.h.RecordSuccess(nodes[0], ResponseMetrics{CacheHit: true, TTFBMs: 90})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// record 2 observations for second node -> change as we now have enough
	th.h.RecordSuccess(nodes[1], ResponseMetrics{CacheHit: true, TTFBMs: 500})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	th.h.RecordSuccess(nodes[1], ResponseMetrics{CacheHit: true, TTFBMs: 90})
	th.updateTiersAndAsert(t, 0, 2, 2, 3, true, []string{nodes[0], nodes[1]})

	// main node gets replaced with unknown node
	th.h.RecordSuccess(nodes[2], ResponseMetrics{CacheHit: true, TTFBMs: 3})
	th.h.RecordSuccess(nodes[2], ResponseMetrics{CacheHit: true, TTFBMs: 5})

	th.updateTiersAndAsert(t, 1, 1, 2, 3, true, []string{nodes[0], nodes[2]})

	// if some main nodes ge kicked out, they get replaced with other eligible nodes

}

func TestIsCorrectnessPolicyEligible(t *testing.T) {
	window := 10

	tcs := map[string]struct {
		perf    *NodePerf
		correct bool
		pct     float64
		initF   func(perf *NodePerf)
	}{
		"no observations": {
			perf:    &NodePerf{},
			correct: true,
		},
		"no success but not enough observations for failure": {
			initF: func(perf *NodePerf) {
				for i := 0; i < window-1; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.nCorrectnessDigest++
				}
			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct: true,
		},
		"some success but fail as enough observations": {
			initF: func(perf *NodePerf) {
				perf.CorrectnessDigest.Append(1)
				perf.nCorrectnessDigest++
				perf.CorrectnessDigest.Append(1)
				perf.nCorrectnessDigest++

				for i := 0; i < int(window)-2; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.nCorrectnessDigest++
				}

			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct: false,
			pct:     20,
		},
		"some success but not enough observations": {
			initF: func(perf *NodePerf) {
				for i := 0; i < int(window)-1; i++ {
					perf.CorrectnessDigest.Append(1)
					perf.nCorrectnessDigest++
				}

			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct: true,
			pct:     0,
		},
		"rolling window": {
			initF: func(perf *NodePerf) {
				// add window success
				for i := 0; i < int(window); i++ {
					perf.CorrectnessDigest.Append(1)
					perf.nCorrectnessDigest++
				}

				// add 2 failures
				for i := 0; i < 2; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.nCorrectnessDigest++
				}

			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct: true,
			pct:     80,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			th := NewTieredHashingHarness(WithWindowSize(window))
			if tc.initF != nil {
				tc.initF(tc.perf)
			}

			perf := tc.perf
			pct, ok := th.h.isCorrectnessPolicyEligible(perf)
			require.EqualValues(t, tc.correct, ok)
			require.EqualValues(t, tc.pct, pct)
		})
	}
}

func TestAddOrchestratorNodes(t *testing.T) {
	th := NewTieredHashingHarness()

	nodes := th.genAndAddAll(t, 10)
	th.assertSize(t, 0, 10)

	nodes2 := th.genNodes(t, 10)
	th.addNewNodesAll(t, nodes2)
	th.assertSize(t, 0, 20)

	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 0, 0, 0, 20)

	th.h.removeFailedNode(nodes[0])
	th.assertSize(t, 0, 19)

	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 0, 1, 0, 19)
}

func TestAddOrchestratorNodesMax(t *testing.T) {
	th := NewTieredHashingHarness(WithMaxPoolSize(10))

	// empty -> 10 get added
	nodes := th.genNodes(t, 30)
	a, _ := th.h.AddOrchestratorNodes(nodes)
	require.EqualValues(t, 10, a)
	th.assertSize(t, 0, 10)

	// nothing gets added as we are full
	nodes2 := th.genNodes(t, 30)
	a, _ = th.h.AddOrchestratorNodes(append(nodes, nodes2...))
	require.EqualValues(t, 0, a)
	th.assertSize(t, 0, 10)

	// remove 2 nodes ->
	th.h.removeFailedNode(nodes[0])
	th.assertSize(t, 0, 9)
	th.h.removeFailedNode(nodes[1])
	th.assertSize(t, 0, 8)

	// 2 get added now
	a, _ = th.h.AddOrchestratorNodes(append(nodes, nodes2...))
	require.EqualValues(t, 2, a)
	th.assertSize(t, 0, 10)

	// removed node does not get added back
	th.h.removeFailedNode(nodes[2])
	th.assertSize(t, 0, 9)

	a, ar := th.h.AddOrchestratorNodes(nodes[:10])
	require.EqualValues(t, 0, a)
	require.EqualValues(t, 3, ar)
	th.assertSize(t, 0, 9)
}

type TieredHashingHarness struct {
	count int
	h     *TieredHashing
}

func NewTieredHashingHarness(opts ...Option) *TieredHashingHarness {
	return &TieredHashingHarness{
		h: New(opts...),
	}
}

func (th *TieredHashingHarness) genAndAddAll(t *testing.T, n int) []string {
	nodes := th.genNodes(t, n)
	th.addNewNodesAll(t, nodes)
	return nodes
}

func (th *TieredHashingHarness) genNodes(t *testing.T, n int) []string {
	var nodes []string
	// generate n random strings
	for i := 0; i < n; i++ {
		nodes = append(nodes, fmt.Sprintf("%d-test", th.count+i))
	}
	th.count = th.count + n
	return nodes
}

func (th *TieredHashingHarness) addNewNodesAll(t *testing.T, nodes []string) {
	var old []string

	for key := range th.h.nodes {
		old = append(old, key)
	}

	added, already := th.h.AddOrchestratorNodes(append(nodes, old...))
	require.Zero(t, already)
	require.EqualValues(t, len(nodes), added)
}

func (th *TieredHashingHarness) assertRemoved(t *testing.T, nodes []string) {
	for _, n := range nodes {
		_, ok := th.h.nodes[n]
		require.False(t, ok)
	}
}

func (th *TieredHashingHarness) addAndAssert(t *testing.T, nodes []string, added, already int, main, unknown int) {
	a, ar := th.h.AddOrchestratorNodes(nodes)
	require.EqualValues(t, added, a)

	require.EqualValues(t, already, ar)
	th.assertSize(t, main, unknown)
}

func (th *TieredHashingHarness) assertSize(t *testing.T, main int, unknown int) {
	mt := th.h.GetPoolMetrics()
	require.EqualValues(t, unknown, mt.Unknown)
	require.EqualValues(t, main, mt.Main)
	require.EqualValues(t, main+unknown, mt.Total)
}

func (th *TieredHashingHarness) nSuccess(node string) int {
	return int(th.h.nodes[node].CorrectnessDigest.Reduce(rolling.Sum))
}
