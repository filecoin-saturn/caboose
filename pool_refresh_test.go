package caboose

import (
	"math/rand"
	"testing"

	"github.com/filecoin-saturn/caboose/tieredhashing"
	"github.com/stretchr/testify/require"
)

func TestPoolRefresh(t *testing.T) {
	opts := []tieredhashing.Option{tieredhashing.WithCorrectnessWindowSize(1), tieredhashing.WithMaxPoolSize(5)}

	p := newPool(&Config{TieredHashingOpts: opts})

	// Add 3 nodes
	nodes := []string{"node1", "node2", "node3"}
	andAndAssertPool(t, p, nodes, 0, 3, 3, 3)

	// add no node
	andAndAssertPool(t, p, nil, 0, 3, 3, 0)

	// add a new node
	andAndAssertPool(t, p, []string{"node4"}, 0, 4, 4, 1)

	// add a new node with already added nodes
	andAndAssertPool(t, p, []string{"node1", "node2", "node3", "node4", "node5"}, 0, 5, 5, 1)

	// record failure so that node is removed and then assert
	rm := p.th.RecordFailure("node4", tieredhashing.ResponseMetrics{ConnFailure: true})
	require.NotNil(t, rm)
	require.EqualValues(t, "node4", rm.Node)

	// removed node is NOT added back as pool is  full without it
	andAndAssertPool(t, p, []string{"node1", "node2", "node3", "node4", "node5", "node6"}, 0, 5, 5, 0)
	nds := p.th.GetPerf()
	for node := range nds {
		require.NotEqual(t, "node4", node)
	}

}

func TestPoolRefreshWithLatencyDistribution(t *testing.T) {
	t.Skip("ENABLE if we go back to tiered hashing")
	opts := []tieredhashing.Option{tieredhashing.WithLatencyWindowSize(2), tieredhashing.WithMaxMainTierSize(2)}

	p := newPool(&Config{TieredHashingOpts: opts})
	nodes := []string{"node1", "node2", "node3"}
	andAndAssertPool(t, p, nodes, 0, 3, 3, 3)

	// record success so a node becomes a main node
	p.th.RecordSuccess("node1", tieredhashing.ResponseMetrics{TTFBMs: 10})
	andAndAssertPool(t, p, nodes, 0, 3, 3, 0)

	p.th.RecordSuccess("node1", tieredhashing.ResponseMetrics{TTFBMs: 20})
	andAndAssertPool(t, p, nodes, 0, 3, 3, 0)

	p.th.RecordSuccess("node2", tieredhashing.ResponseMetrics{TTFBMs: 30})
	p.th.RecordSuccess("node2", tieredhashing.ResponseMetrics{TTFBMs: 40})
	andAndAssertPool(t, p, nodes, 2, 1, 3, 0)
}

func andAndAssertPool(t *testing.T, p *pool, nodes []string, expectedMain, expectedUnknown, expectedTotal, expectedNew int) {

	parsedNodes := make([]tieredhashing.NodeInfo, 0)

	for _, n := range nodes {
		parsedNodes = append(parsedNodes, tieredhashing.NodeInfo{
			IP:            n,
			ID:            n,
			Weight:        rand.Intn(100),
			Distance:      rand.Float32(),
			ComplianceCid: n,
		})
	}

	p.refreshWithNodes(parsedNodes)
	nds := p.th.GetPerf()
	require.Equal(t, expectedTotal, len(nds))
	mts := p.th.GetPoolMetrics()

	require.EqualValues(t, expectedMain, mts.Main)
	require.EqualValues(t, expectedUnknown, mts.Unknown)
	require.EqualValues(t, expectedTotal, mts.Total)
}
