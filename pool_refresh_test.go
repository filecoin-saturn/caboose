package caboose

import (
	"testing"

	"github.com/filecoin-saturn/caboose/tieredhashing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPoolRefresh(t *testing.T) {
	opts := []tieredhashing.Option{tieredhashing.WithCorrectnessWindowSize(1)}

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

	require.EqualValues(t, 0, testutil.ToFloat64(poolMembersNotAddedBecauseRemovedMetric))
	// record failure so that node is removed and then assert
	rm := p.th.RecordFailure("node4", tieredhashing.ResponseMetrics{ConnFailure: true})
	require.NotNil(t, rm)
	andAndAssertPool(t, p, []string{"node1", "node2", "node3", "node4", "node5"}, 0, 4, 4, 0)
	require.EqualValues(t, 1, testutil.ToFloat64(poolMembersNotAddedBecauseRemovedMetric))
}

func TestPoolRefreshWithLatencyDistribution(t *testing.T) {
	opts := []tieredhashing.Option{tieredhashing.WithLatencyWindowSize(2), tieredhashing.WithMaxMainTierSize(2)}

	p := newPool(&Config{TieredHashingOpts: opts})
	nodes := []string{"node1", "node2", "node3"}
	andAndAssertPool(t, p, nodes, 0, 3, 3, 3)
	require.EqualValues(t, 0, testutil.ToFloat64(poolTierChangMetric.WithLabelValues("main-to-unknown")))
	require.EqualValues(t, 0, testutil.ToFloat64(poolTierChangMetric.WithLabelValues("unknown-to-main")))

	// record success so a node becomes a main node
	p.th.RecordSuccess("node1", tieredhashing.ResponseMetrics{TTFBMs: 10})
	andAndAssertPool(t, p, nodes, 0, 3, 3, 0)

	p.th.RecordSuccess("node1", tieredhashing.ResponseMetrics{TTFBMs: 20})
	andAndAssertPool(t, p, nodes, 0, 3, 3, 0)

	p.th.RecordSuccess("node2", tieredhashing.ResponseMetrics{TTFBMs: 30})
	p.th.RecordSuccess("node2", tieredhashing.ResponseMetrics{TTFBMs: 40})
	andAndAssertPool(t, p, nodes, 2, 1, 3, 0)
	require.EqualValues(t, 0, testutil.ToFloat64(poolTierChangMetric.WithLabelValues("main-to-unknown")))
	require.EqualValues(t, 2, testutil.ToFloat64(poolTierChangMetric.WithLabelValues("unknown-to-main")))
}

func andAndAssertPool(t *testing.T, p *pool, nodes []string, expectedMain, expectedUnknown, expectedTotal, expectedNew int) {
	p.refreshWithNodes(nodes)
	nds := p.th.GetPerf()
	require.Equal(t, expectedTotal, len(nds))
	mts := p.th.GetPoolMetrics()

	require.EqualValues(t, expectedMain, mts.Main)
	require.EqualValues(t, expectedUnknown, mts.Unknown)
	require.EqualValues(t, expectedTotal, mts.Total)

	// compare metrics
	mains := poolSizeMetric.WithLabelValues("main")
	unknowns := poolSizeMetric.WithLabelValues("unknown")

	require.EqualValues(t, expectedMain, testutil.ToFloat64(mains))
	require.EqualValues(t, expectedUnknown, testutil.ToFloat64(unknowns))
	require.EqualValues(t, expectedNew, testutil.ToFloat64(poolNewMembersMetric))
}
