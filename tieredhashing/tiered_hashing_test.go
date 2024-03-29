package tieredhashing

import (
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"testing"

	"github.com/asecurityteam/rolling"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestRecordSuccess(t *testing.T) {
	window := 3

	th := NewTieredHashingHarness(WithLatencyWindowSize(window), WithFailureDebounce(0))
	// main node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	// cache hits
	th.recordCacheHitAndAssertSet(t, unknownNode, 200, 0, 1, string(TierUnknown))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].NLatencyDigest)
	require.EqualValues(t, 200, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 150})
	require.EqualValues(t, 2, th.h.nodes[unknownNode].NLatencyDigest)
	require.EqualValues(t, 350, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 50})
	require.EqualValues(t, 3, th.h.nodes[unknownNode].NLatencyDigest)
	require.EqualValues(t, 400, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	// windowing
	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 60})
	require.EqualValues(t, 4, th.h.nodes[unknownNode].NLatencyDigest)
	require.EqualValues(t, 260, th.h.nodes[unknownNode].LatencyDigest.Reduce(rolling.Sum))

	// node gets removed for unacceptable latency
	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 600})
	th.assertSize(t, 0, 1)

	// node does not get removed for unacceptable latency if not enough observations
	unknownNode = th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 2)
	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 6000})

	th.assertSize(t, 0, 2)

	th.h.RecordSuccess(unknownNode, ResponseMetrics{TTFBMs: 6000})
}

func (th *TieredHashingHarness) recordCacheHitAndAssertSet(t *testing.T, node string, ttfbMS float64, mc, uc int, tier string) {
	prevSuccess := th.nSuccess(node)
	th.h.RecordSuccess(node, ResponseMetrics{TTFBMs: ttfbMS})
	require.EqualValues(t, prevSuccess+1, th.nSuccess(node))
	th.assertSize(t, mc, uc)
	require.EqualValues(t, th.h.nodes[node].Tier, tier)
}

func TestRecordFailure(t *testing.T) {
	window := 3

	th := NewTieredHashingHarness(WithCorrectnessWindowSize(window), WithFailureDebounce(0), WithMaxMainTierSize(1), WithLatencyWindowSize(2))
	// unknown node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	// 502 status code no change
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ResponseCode: http.StatusBadGateway}))
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ResponseCode: http.StatusGatewayTimeout}))
	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{ConnFailure: true}))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].connFailures)

	require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true}))
	require.EqualValues(t, 1, th.h.nodes[unknownNode].networkErrors)

	// node is evicted as we have enough observations and it's correctness is below threshold acceptance
	th.h.AverageCorrectnessPct = 80
	rm := th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true})
	require.NotNil(t, rm)
	require.EqualValues(t, TierUnknown, rm.Tier)
	require.EqualValues(t, unknownNode, rm.Node)

	// when main node is removed, it is replaced
	nodes := th.genAndAddAll(t, 2)
	mn := nodes[0]
	node2 := nodes[1]
	th.assertSize(t, 0, 2)
	th.h.RecordSuccess(mn, ResponseMetrics{TTFBMs: 100})
	th.h.RecordSuccess(mn, ResponseMetrics{TTFBMs: 150})

	th.h.RecordSuccess(node2, ResponseMetrics{TTFBMs: 100})
	th.h.RecordSuccess(node2, ResponseMetrics{TTFBMs: 150})
	th.h.RecordFailure(mn, ResponseMetrics{NetworkError: true})
	th.h.RecordFailure(mn, ResponseMetrics{NetworkError: true})
	th.h.RecordFailure(mn, ResponseMetrics{NetworkError: true})
	th.assertSize(t, 0, 1)
}

func TestMoveBestUnknownToMain(t *testing.T) {
	th := NewTieredHashingHarness()
	require.Zero(t, th.h.MoveBestUnknownToMain())

	nodes := th.genAndAddAll(t, 2)

	th.assertSize(t, 0, 2)
	th.h.RecordSuccess(nodes[0], ResponseMetrics{TTFBMs: 100})
	th.h.RecordSuccess(nodes[1], ResponseMetrics{TTFBMs: 50})

	require.EqualValues(t, 1, th.h.MoveBestUnknownToMain())
	th.assertSize(t, 1, 1)

	th.h.nodes[nodes[1]].Tier = TierMain
	th.h.nodes[nodes[0]].Tier = TierUnknown
}

func TestComplianceCids(t *testing.T) {

	th := NewTieredHashingHarness()

	nodes := th.genAndAddAll(t, 10)
	th.h.AddOrchestratorNodes(genNodeStructs(nodes))

	t.Run("compliance cids exist for existing nodes", func(t *testing.T) {
		for _, node := range nodes {
			_, err := th.h.GetComplianceCid(node)
			assert.NoError(t, err, "Compliance Cids should always exist for nodes that are part of the pool")
		}
	})

	newNodes := []string{"new-node1", "new-node2"}
	th.addNewNodesAll(t, newNodes)
	th.h.AddOrchestratorNodes(genNodeStructs(newNodes))
	t.Run("compliance cids exist for new nodes", func(t *testing.T) {
		for _, node := range newNodes {
			_, err := th.h.GetComplianceCid(node)
			assert.NoError(t, err, "Compliance Cids should always exist for new added nodes")

		}
	})

	for _, node := range newNodes {
		th.h.removeFailedNode(node)
	}

	t.Run("compliance cids do not exist for removed nodes", func(t *testing.T) {
		for _, node := range newNodes {
			_, err := th.h.GetComplianceCid(node)
			assert.Error(t, err, "Compliance cids do not exist for removed nodes")
		}
	})
}

func TestNodeNotRemovedWithVar(t *testing.T) {
	window := 2
	th := NewTieredHashingHarness(WithCorrectnessWindowSize(window), WithFailureDebounce(0), WithNoRemove(true))
	// unknown node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	for i := 0; i < 1000; i++ {
		require.Nil(t, th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true}))
	}
	th.assertSize(t, 0, 1)
}

func TestUpdateAverageCorrectnessPct(t *testing.T) {
	window := 2

	th := NewTieredHashingHarness(WithCorrectnessWindowSize(window), WithFailureDebounce(0), WithCorrectnessThreshold(30))
	node := th.genAndAddAll(t, 1)[0]

	th.h.UpdateAverageCorrectnessPct()
	require.Zero(t, th.h.AverageCorrectnessPct)

	th.h.RecordSuccess(node, ResponseMetrics{Success: true})
	th.h.UpdateAverageCorrectnessPct()
	require.Zero(t, th.h.AverageCorrectnessPct)

	th.h.RecordSuccess(node, ResponseMetrics{Success: true})
	th.h.UpdateAverageCorrectnessPct()
	require.EqualValues(t, 100, th.h.AverageCorrectnessPct)

	th.h.RecordFailure(node, ResponseMetrics{NetworkError: true})
	th.h.UpdateAverageCorrectnessPct()
	require.EqualValues(t, 50, th.h.AverageCorrectnessPct)
}

func TestNodeEvictionWithWindowing(t *testing.T) {
	window := 4

	th := NewTieredHashingHarness(WithCorrectnessWindowSize(window), WithFailureDebounce(0), WithCorrectnessThreshold(30))
	// main node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 0, 1)

	th.h.mainSet.AddNode(unknownNode)
	th.h.unknownSet.RemoveNode(unknownNode)
	th.h.nodes[unknownNode].Tier = TierMain

	// record success
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	rm := th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true})
	require.Nil(t, rm)

	// evicted as pct < 80 because of windowing
	th.h.AverageCorrectnessPct = 100
	rm = th.h.RecordFailure(unknownNode, ResponseMetrics{NetworkError: true})
	require.NotNil(t, rm)
	require.EqualValues(t, TierMain, rm.Tier)
	require.EqualValues(t, unknownNode, rm.Node)
}

func TestGetNodes(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	assertCountF := func(t *testing.T, resp []string, mc, uc int) {
		var countMain int
		var countUnknown int
		for _, n := range resp {
			if th.h.nodes[n].Tier == TierMain {
				countMain++
			} else {
				countUnknown++
			}
		}
		require.EqualValues(t, mc, countMain)
		require.EqualValues(t, uc, countUnknown)
	}

	// empty
	nds := th.h.GetNodes(TierMain, "test", 1)
	require.Empty(t, nds)

	// has 3 unknown, 0 main
	unknownNodes := th.genAndAddAll(t, 3)
	th.assertSize(t, 0, 3)
	resp := th.h.GetNodes(TierUnknown, "test", 100)
	require.Len(t, resp, 3)
	assertCountF(t, resp, 0, 3)

	// has 2 main, 3 unknown
	mainNodes := th.genAndAddAll(t, 2)
	for _, n := range mainNodes {
		th.h.nodes[n].Tier = TierMain
		th.h.mainSet = th.h.mainSet.AddNode(n)
		th.h.unknownSet = th.h.unknownSet.RemoveNode(n)
	}

	th.assertSize(t, 2, 3)
	resp = th.h.GetNodes(TierMain, "test", 100)
	require.Len(t, resp, 2)
	assertCountF(t, resp, 2, 0)

	assertGetAndCountF := func(t *testing.T, mainS int, unknownS int, n int, mc, uc int) {
		th.assertSize(t, mainS, unknownS)
		resp = th.h.GetNodes(TierMain, "test", n)
		require.Len(t, resp, mc)
		assertCountF(t, resp, mc, 0)
		resp = th.h.GetNodes(TierUnknown, "test", n)
		require.Len(t, resp, uc)
		assertCountF(t, resp, 0, uc)
	}

	// has both main
	assertGetAndCountF(t, 2, 3, 2, 2, 2)

	th.h.removeFailedNode(mainNodes[0])
	assertGetAndCountF(t, 1, 3, 10, 1, 3)

	// has 1 main, 1 unknown
	assertGetAndCountF(t, 1, 3, 2, 1, 2)

	// has 1 main, 0 unknown
	assertGetAndCountF(t, 1, 3, 1, 1, 1)

	// has 1 main, 2 unknown
	th.h.removeFailedNode(unknownNodes[0])
	assertGetAndCountF(t, 1, 2, 10, 1, 2)

	// has 0 main, 1 unknown
	th.h.removeFailedNode(mainNodes[1])
	assertGetAndCountF(t, 0, 2, 1, 0, 1)

	// has 0 main, 0 unknown
	th.h.removeFailedNode(unknownNodes[1])
	th.h.removeFailedNode(unknownNodes[2])
	assertGetAndCountF(t, 0, 0, 1, 0, 0)
}

func TestConsistentHashing(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	th.genAndAddAll(t, 10)
	th.assertSize(t, 0, 10)
	resp1 := th.h.GetNodes(TierUnknown, "test", 3)
	require.Len(t, resp1, 3)

	resp2 := th.h.GetNodes(TierMain, "test", 2)
	require.Len(t, resp2, 0)
}

func TestRecordCorrectness(t *testing.T) {
	window := 3
	th := NewTieredHashingHarness(WithCorrectnessWindowSize(window))
	perf := &NodePerf{
		CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
	}
	th.h.recordCorrectness(perf, true)
	require.EqualValues(t, 1, perf.NCorrectnessDigest)
	require.EqualValues(t, 1, perf.CorrectnessDigest.Reduce(rolling.Sum))

	th.h.recordCorrectness(perf, true)
	require.EqualValues(t, 2, perf.CorrectnessDigest.Reduce(rolling.Sum))
	require.EqualValues(t, 2, perf.NCorrectnessDigest)

	th.h.recordCorrectness(perf, false)
	require.EqualValues(t, 3, perf.NCorrectnessDigest)
	require.EqualValues(t, 2, perf.CorrectnessDigest.Reduce(rolling.Sum))

	th.h.recordCorrectness(perf, false)
	require.EqualValues(t, 3, perf.NCorrectnessDigest)
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
		if perf.Tier == TierMain {
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
	t.Skip("we probably dont need this will we turn on tiered hashing")
	windowSize := 2
	th := NewTieredHashingHarness(WithLatencyWindowSize(windowSize), WithMaxMainTierSize(2))

	mc, uc := th.h.UpdateMainTierWithTopN()
	require.Zero(t, mc)
	require.Zero(t, uc)

	// main node
	nodes := th.genAndAddAll(t, 5)
	th.assertSize(t, 0, 5)

	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// Record 1 observation for a node -> no change
	th.h.RecordSuccess(nodes[0], ResponseMetrics{TTFBMs: 100})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// record 1 more observation for the same node -> no change as not enough nodes for bulk update
	th.h.RecordSuccess(nodes[0], ResponseMetrics{TTFBMs: 90})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	// record 2 observations for second node -> change as we now have enough
	th.h.RecordSuccess(nodes[1], ResponseMetrics{TTFBMs: 500})
	th.updateTiersAndAsert(t, 0, 0, 0, 5, false, nil)

	th.h.RecordSuccess(nodes[1], ResponseMetrics{TTFBMs: 90})
	th.updateTiersAndAsert(t, 0, 2, 2, 3, true, []string{nodes[0], nodes[1]})

	// main node gets replaced with unknown node
	th.h.RecordSuccess(nodes[2], ResponseMetrics{TTFBMs: 3})
	th.h.RecordSuccess(nodes[2], ResponseMetrics{TTFBMs: 5})

	th.updateTiersAndAsert(t, 1, 1, 2, 3, true, []string{nodes[0], nodes[2]})

	// say have less than N eligible nodes
	th.h.removeFailedNode(nodes[0])
	th.h.removeFailedNode(nodes[1])
	th.h.removeFailedNode(nodes[2])
	th.updateTiersAndAsert(t, 0, 0, 0, 2, true, nil)

	// update works even with 1 node
	th.h.RecordSuccess(nodes[3], ResponseMetrics{TTFBMs: 3})
	th.h.RecordSuccess(nodes[3], ResponseMetrics{TTFBMs: 5})
	th.updateTiersAndAsert(t, 0, 1, 1, 1, true, []string{nodes[3]})

	th.h.removeFailedNode(nodes[3])
	th.updateTiersAndAsert(t, 0, 0, 0, 1, true, nil)
	th.h.RecordSuccess(nodes[4], ResponseMetrics{TTFBMs: 3})
	th.h.RecordSuccess(nodes[4], ResponseMetrics{TTFBMs: 5})
	th.updateTiersAndAsert(t, 0, 1, 1, 0, true, []string{nodes[4]})
	th.h.removeFailedNode(nodes[4])
	th.updateTiersAndAsert(t, 0, 0, 0, 0, true, nil)
}

func TestIsCorrectnessPolicyEligible(t *testing.T) {
	window := 10

	tcs := map[string]struct {
		perf                  *NodePerf
		correct               bool
		pct                   float64
		initF                 func(perf *NodePerf)
		averageCorrectnessPct float64
	}{
		"no observations": {
			perf:    &NodePerf{},
			correct: true,
		},
		"no success but not enough observations for failure": {
			initF: func(perf *NodePerf) {
				for i := 0; i < window-1; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.NCorrectnessDigest++
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
				perf.NCorrectnessDigest++
				perf.CorrectnessDigest.Append(1)
				perf.NCorrectnessDigest++

				for i := 0; i < int(window)-2; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.NCorrectnessDigest++
				}

			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct:               false,
			pct:                   20,
			averageCorrectnessPct: 45,
		},
		"some success and success as above threshold": {
			initF: func(perf *NodePerf) {
				perf.CorrectnessDigest.Append(1)
				perf.NCorrectnessDigest++
				perf.CorrectnessDigest.Append(1)
				perf.NCorrectnessDigest++

				for i := 0; i < int(window)-2; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.NCorrectnessDigest++
				}

			},
			perf: &NodePerf{
				CorrectnessDigest: rolling.NewPointPolicy(rolling.NewWindow(int(window))),
			},
			correct:               true,
			pct:                   20,
			averageCorrectnessPct: 40,
		},
		"some success but not enough observations": {
			initF: func(perf *NodePerf) {
				for i := 0; i < int(window)-1; i++ {
					perf.CorrectnessDigest.Append(1)
					perf.NCorrectnessDigest++
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
					perf.NCorrectnessDigest++
				}

				// add 2 failures
				for i := 0; i < 2; i++ {
					perf.CorrectnessDigest.Append(0)
					perf.NCorrectnessDigest++
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
			th := NewTieredHashingHarness(WithCorrectnessWindowSize(window))
			if tc.initF != nil {
				tc.initF(tc.perf)
			}
			th.h.AverageCorrectnessPct = tc.averageCorrectnessPct

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

	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 0, 0, 0, 0, 20)

	th.h.removeFailedNode(nodes[0])
	th.assertSize(t, 0, 19)

	// removed node gets added back as we are not full
	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 1, 1, 1, 0, 20)
}

func TestAddOrchestratorNodesMax(t *testing.T) {
	th := NewTieredHashingHarness(WithMaxPoolSize(10))

	// empty -> 10 get added
	nodes := th.genNodes(t, 30)
	a, _, _ := th.h.AddOrchestratorNodes(genNodeStructs(nodes))
	require.EqualValues(t, 10, a)
	th.assertSize(t, 0, 10)

	// nothing gets added as we are full
	nodes2 := th.genNodes(t, 30)
	a, _, _ = th.h.AddOrchestratorNodes(append(genNodeStructs(nodes), genNodeStructs(nodes2)...))
	require.EqualValues(t, 0, a)
	th.assertSize(t, 0, 10)

	// remove 2 nodes ->
	th.h.removeFailedNode(nodes[0])
	th.assertSize(t, 0, 9)
	th.h.removeFailedNode(nodes[1])
	th.assertSize(t, 0, 8)

	// 2 get added now
	a, _, _ = th.h.AddOrchestratorNodes(append(genNodeStructs(nodes), genNodeStructs(nodes2)...))
	require.EqualValues(t, 2, a)
	th.assertSize(t, 0, 10)

	th.h.removeFailedNode(nodes[2])
	th.assertSize(t, 0, 9)

	// removed node does not get added back as we are already full without it
	a, ar, back := th.h.AddOrchestratorNodes(append(genNodeStructs(nodes), genNodeStructs([]string{"newNode"})...))
	require.EqualValues(t, 1, a)
	require.EqualValues(t, 3, ar)
	th.assertSize(t, 0, 10)
	require.EqualValues(t, 0, back)
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

func genNodeStructs(nodes []string) []NodeInfo {
	var nodeStructs []NodeInfo

	for _, node := range nodes {
		cid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte(node))
		nodeStructs = append(nodeStructs, NodeInfo{
			IP:            node,
			ID:            node,
			Weight:        rand.Intn(100),
			Distance:      rand.Float32(),
			ComplianceCid: cid.String(),
		})
	}
	return nodeStructs
}

func (th *TieredHashingHarness) addNewNodesAll(t *testing.T, nodes []string) {
	var old []string

	for key := range th.h.nodes {
		old = append(old, key)
	}

	added, already, _ := th.h.AddOrchestratorNodes(append(genNodeStructs(nodes), genNodeStructs(old)...))
	require.Zero(t, already)
	require.EqualValues(t, len(nodes), added)
}

func (th *TieredHashingHarness) addAndAssert(t *testing.T, nodes []string, added, already, ab int, main, unknown int) {
	a, ar, addedBack := th.h.AddOrchestratorNodes(genNodeStructs(nodes))
	require.EqualValues(t, added, a)

	require.EqualValues(t, already, ar)
	th.assertSize(t, main, unknown)

	require.EqualValues(t, ab, addedBack)
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
