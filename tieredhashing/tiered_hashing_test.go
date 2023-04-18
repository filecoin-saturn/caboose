package tieredhashing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordSuccessSimple(t *testing.T) {
	th := NewTieredHashingHarness()
	// main node
	mainNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 1, 0)

	// unknown node
	unknownNode := th.genAndAddAll(t, 1)[0]
	th.assertSize(t, 1, 1)

	// cache miss -> record success but latency no-op
	th.h.RecordSuccess(mainNode, ResponseMetrics{})
	require.EqualValues(t, th.h.nodes[mainNode].nSuccess, 1)
	th.assertSize(t, 1, 1)

	th.h.RecordSuccess(unknownNode, ResponseMetrics{})
	require.EqualValues(t, th.h.nodes[unknownNode].nSuccess, 1)
	th.assertSize(t, 1, 1)

	// main node cache hit -> becomes unknown as bad latency
	temp := mainNode
	th.recordCacheHitAndAssertSet(t, mainNode, maxLatencyForMainSetMillis+1, 0, 2, tierUnknown)

	// unknown node cache hit -> become main as latency is good
	th.recordCacheHitAndAssertSet(t, unknownNode, maxLatencyForMainSetMillis-1, 1, 1, tierMain)
	mainNode = unknownNode
	unknownNode = temp

	// unknown node -> improve percentile -> becomes main node
	th.recordCacheHitAndAssertSet(t, unknownNode, maxLatencyForMainSetMillis-1, 1, 1, tierUnknown)
	th.recordCacheHitAndAssertSet(t, unknownNode, maxLatencyForMainSetMillis-1, 1, 1, tierUnknown)
	// works now
	th.recordCacheHitAndAssertSet(t, unknownNode, maxLatencyForMainSetMillis-1, 2, 0, tierMain)
}

func TestRecordSuccessFailureDebounce(t *testing.T) {
	th := NewTieredHashingHarness()
	mainNode := th.genAndAddAll(t, 1)[0]

	// record success values
	th.recordCacheHitAndAssertSet(t, mainNode, maxLatencyForMainSetMillis-1, 1, 0, tierMain)
	th.recordCacheHitAndAssertSet(t, mainNode, maxLatencyForMainSetMillis-1, 1, 0, tierMain)
	th.recordCacheHitAndAssertSet(t, mainNode, maxLatencyForMainSetMillis-1, 1, 0, tierMain)

	// no tier change because of debounce
	for i := 0; i < 10; i++ {
		th.recordCacheHitAndAssertSet(t, mainNode, maxLatencyForMainSetMillis+1, 1, 0, tierMain)
	}
	// have atleast 10 observations
}

func TestRecordSuccessWindowing(t *testing.T) {
}

func (th *TieredHashingHarness) recordCacheHitAndAssertSet(t *testing.T, node string, ttfbMS float64, mc, uc int, tier string) {
	prevSuccess := th.h.nodes[node].nSuccess
	th.h.RecordSuccess(node, ResponseMetrics{CacheHit: true, TTFBMs: ttfbMS})
	require.EqualValues(t, prevSuccess+1, th.h.nodes[node].nSuccess)
	th.assertSize(t, mc, uc)

	require.EqualValues(t, th.h.nodes[node].tier, tier)
}

func TestGetNodes(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	assertCountF := func(t *testing.T, resp []string, mc, uc int) {
		var countMain int
		var countUnknown int
		for _, n := range resp {
			if th.h.nodes[n].tier == tierMain {
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

	// has 2 main, 0 unknown
	mainNodes := th.genAndAddAll(t, 2)
	th.assertSize(t, 2, 0)
	resp := th.h.GetNodes("test", 100)
	require.Len(t, resp, 2)
	assertCountF(t, resp, 2, 0)

	// has 2 main, 3 unknown
	unknownNodes := th.genAndAddAll(t, 3)
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

	// has 1 main, 3 unknown
	th.h.removeFailed(mainNodes[0])
	assertGetAndCountF(t, 1, 3, 10, 4, 1, 3)

	// has 1 main, 1 unknown
	assertGetAndCountF(t, 1, 3, 2, 2, 1, 1)

	// has 1 main, 0 unknown
	assertGetAndCountF(t, 1, 3, 1, 1, 1, 0)

	// has 1 main, 2 unknown
	th.h.removeFailed(unknownNodes[0])
	assertGetAndCountF(t, 1, 2, 10, 3, 1, 2)

	// has 0 main, 1 unknown
	th.h.removeFailed(mainNodes[1])
	assertGetAndCountF(t, 0, 2, 1, 1, 0, 1)

	// has 0 main, 0 unknown
	th.h.removeFailed(unknownNodes[1])
	th.h.removeFailed(unknownNodes[2])
	assertGetAndCountF(t, 0, 0, 1, 0, 0, 0)
}

func TestConsistentHashing(t *testing.T) {
	th := NewTieredHashingHarness(WithAlwaysMainFirst())

	th.genAndAddAll(t, 10)
	th.assertSize(t, 10, 0)
	resp1 := th.h.GetNodes("test", 3)
	require.Len(t, resp1, 3)

	resp2 := th.h.GetNodes("test", 2)
	require.Len(t, resp2, 2)

	require.EqualValues(t, resp1[:2], resp2[:2])
}

func TestAddOrchestratorNodes(t *testing.T) {
	th := NewTieredHashingHarness()

	nodes := th.genAndAddAll(t, 10)
	th.assertSize(t, 10, 0)

	nodes2 := th.genNodes(t, 10)
	th.addNewNodesAll(t, nodes2)
	th.assertSize(t, 10, 10)

	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 0, 14, 0, 3, 3)

	th.h.removeFailed(nodes[0])
	th.assertSize(t, 2, 3)

	th.addAndAssert(t, append(nodes[:3], nodes2[:3]...), 0, 0, 1, 2, 3)
}

func TestOrchestratorAddMaxNodes(t *testing.T) {
	th := NewTieredHashingHarness(WithMaxPoolSizeEmpty(20), WithMaxPoolSizeNonEmpty(10))

	// empty -> only 20 get added
	nodes := th.genNodes(t, 30)
	a, _, _ := th.h.AddOrchestratorNodes(nodes)
	require.EqualValues(t, 20, a)
	th.assertSize(t, 20, 0)

	// non empty -> nothing gets added
	nodes2 := th.genNodes(t, 30)
	a, _, _ = th.h.AddOrchestratorNodes(append(nodes, nodes2...))
	require.EqualValues(t, 0, a)
	th.assertSize(t, 20, 0)

	// remove 12 nodes ->
	r := th.h.removeNodesNotInOrchestrator(nodes[:8])
	require.EqualValues(t, 12, r)
	th.assertSize(t, 8, 0)

	// 2 get added now
	a, _, _ = th.h.AddOrchestratorNodes(append(nodes, nodes2...))
	require.EqualValues(t, 2, a)
	th.assertSize(t, 8, 2)
}

func TestRemoveNodesNotInOrchestrator(t *testing.T) {
	th := NewTieredHashingHarness()
	nodes := th.genAndAddAll(t, 10)
	th.assertSize(t, 10, 0)

	// give only a subset now
	r := th.h.removeNodesNotInOrchestrator(nodes[:8])
	require.EqualValues(t, 2, r)
	th.assertRemoved(t, nodes[8:])
	th.assertSize(t, 8, 0)

	r = th.h.removeNodesNotInOrchestrator(nodes[:8])
	require.EqualValues(t, 0, r)
	th.assertSize(t, 8, 0)

	r = th.h.removeNodesNotInOrchestrator(nodes[:8])
	require.EqualValues(t, 0, r)
	th.assertSize(t, 8, 0)

	r = th.h.removeNodesNotInOrchestrator(nil)
	require.EqualValues(t, 8, r)
	th.assertRemoved(t, nodes)
	th.assertSize(t, 0, 0)
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

	added, removed, already := th.h.AddOrchestratorNodes(append(nodes, old...))
	require.Zero(t, removed)
	require.Zero(t, already)
	require.EqualValues(t, len(nodes), added)
}

func (th *TieredHashingHarness) assertRemoved(t *testing.T, nodes []string) {
	for _, n := range nodes {
		_, ok := th.h.nodes[n]
		require.False(t, ok)
	}
}

func (th *TieredHashingHarness) addAndAssert(t *testing.T, nodes []string, added, removed, already int, main, unknown int) {
	a, r, ar := th.h.AddOrchestratorNodes(nodes)
	require.EqualValues(t, added, a)
	require.EqualValues(t, removed, r)
	require.EqualValues(t, already, ar)
	th.assertSize(t, main, unknown)
}

func (th *TieredHashingHarness) assertSize(t *testing.T, main int, unknown int) {
	mt := th.h.GetPoolMetrics()
	require.EqualValues(t, unknown, mt.Unknown)
	require.EqualValues(t, main, mt.Main)
	require.EqualValues(t, main+unknown, mt.Total)
}
