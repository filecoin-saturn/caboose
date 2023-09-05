package caboose_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/filecoin-saturn/caboose/internal/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
)

const (
	nodesSize = 6
)

/*
This function tests if the caboose pool converges to a set of nodes that are expected
based on given controled scenarios. The function continuously injects stats into
certain nodes and simulates the caboose pool refreshing over time and updating its
active set of nodes based on the stats injected.

The tests are designed such that there is two groups of nodes: "bad", and "good". Those
are picked randomly in the beginning of each test. At the end of each test, the pool should
always be converging to the "good" nodes.
*/
func TestPoolDynamics(t *testing.T) {
	baseStatSize := 100000
	baseStatLatency := 100
	poolRefreshNo := 10
	ctx := context.Background()
	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)

	// This test ensures that when the pool is intialized, it should converge to a set
	// of nodes that have stats vs a set of nodes that don't have any stats.
	t.Run("pool converges to good nodes vs nodes with no stats", func(t *testing.T) {
		ch, controlGroup := getHarnessAndControlGroup(t, nodesSize, nodesSize/2)
		ch.FetchAndAssertSuccess(t, ctx, testCid)

		goodNodes := make([]*caboose.Node, 0)
		badNodes := make([]*caboose.Node, 0)
		for _, n := range ch.CabooseAllNodes.Nodes {
			_, ok := controlGroup[n.URL]
			if ok {
				goodNodes = append(goodNodes, n)
			} else {
				badNodes = append(badNodes, n)
			}
		}

		for i := 0; i < 1; i++ {
			nodes := make([]string, 0)
			for _, n := range ch.CabooseAllNodes.Nodes {
				nodes = append(nodes, n.URL)
			}
			fmt.Println("All nodes", nodes)

			goodStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}

			bn := make([]string, 0)
			gn := make([]string, 0)
			for _, n := range goodNodes {
				gn = append(gn, n.URL)
			}

			for _, n := range badNodes {
				bn = append(bn, n.URL)
			}
			fmt.Println("Good Nodes", gn)
			fmt.Println("Bad nodes", bn)

			ch.RecordSuccesses(t, goodNodes, goodStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		for n := range controlGroup {
			assert.Contains(t, ch.CabooseActiveNodes.Nodes, n)
		}

		np := make([]string, 0)
		for _, n := range ch.CabooseActiveNodes.Nodes {
			np = append(np, n.URL)
		}

		fmt.Println("Final Node Pool", np)

		for _, n := range ch.CabooseAllNodes.Nodes {
			fmt.Println("Node", n.URL, "Priority", n.Priority(), "Rate", n.Rate(), "samples ", len(n.Samples.PeekAll()))
		}

	})

	t.Run("pool converges to good nodes vs nodes with worse stats", func(t *testing.T) {
		ch, controlGroup := getHarnessAndControlGroup(t, nodesSize, nodesSize/2)

		goodNodes := make([]*caboose.Node, 0)
		badNodes := make([]*caboose.Node, 0)
		for _, n := range ch.CabooseAllNodes.Nodes {
			_, ok := controlGroup[n.URL]
			if ok {
				goodNodes = append(goodNodes, n)
			} else {
				badNodes = append(badNodes, n)
			}
		}

		for i := 0; i < poolRefreshNo; i++ {

			goodStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}
			badStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) * float64(10),
				Size:    float64(baseStatSize) / float64(10),
			}

			ch.RecordSuccesses(t, goodNodes, goodStats, 1000)
			ch.RecordSuccesses(t, badNodes, badStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		for n := range controlGroup {
			assert.Contains(t, ch.CabooseActiveNodes.Nodes, n)
		}

	})

	// When new nodes join, if they start consistently performing better than the nodes in the current pool,
	// then those nodes should replace the nodes in the current pool.
	t.Run("pool converges to new nodes that are better than the current pool", func(t *testing.T) {
		ch, controlGroup := getHarnessAndControlGroup(t, nodesSize, nodesSize/2)
		goodNodes := make([]*caboose.Node, 0)
		badNodes := make([]*caboose.Node, 0)

		for _, n := range ch.CabooseAllNodes.Nodes {
			_, ok := controlGroup[n.URL]
			if ok {
				goodNodes = append(goodNodes, n)
			} else {
				badNodes = append(badNodes, n)
			}
		}

		// Give the bad nodes some stats, those nodes then become the main active tier.
		// The good nodes have 0 stats after this should not be picked at this point.
		for i := 0; i < poolRefreshNo; i++ {
			badStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) * float64(10),
				Size:    float64(baseStatSize) / float64(10),
			}
			ch.RecordSuccesses(t, badNodes, badStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		// Add some new "good" nodes that have better stats over a longer period of time.
		for i := 0; i < poolRefreshNo*2; i++ {
			goodStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}
			ch.RecordSuccesses(t, goodNodes, goodStats, 2000)
			ch.CaboosePool.DoRefresh()
		}

		ch.CaboosePool.DoRefresh()
		for n := range controlGroup {
			assert.Contains(t, ch.CabooseActiveNodes.Nodes, n)
		}

	})

	// If the current active main pool starts failing, the pool should converge to
	// to nodes that are not failing.
	t.Run("pool converges to other nodes if the current ones start failing", func(t *testing.T) {
		ch, controlGroup := getHarnessAndControlGroup(t, nodesSize, nodesSize/2)
		goodNodes := make([]*caboose.Node, 0)
		badNodes := make([]*caboose.Node, 0)

		for _, n := range ch.CabooseAllNodes.Nodes {
			_, ok := controlGroup[n.URL]
			if ok {
				goodNodes = append(goodNodes, n)
			} else {
				badNodes = append(badNodes, n)
			}
		}

		// Start with the bad nodes having better stats than the good nodes
		for i := 0; i < poolRefreshNo; i++ {
			goodStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}
			badStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) * float64(10),
				Size:    float64(baseStatSize) / float64(10),
			}

			ch.RecordSuccesses(t, goodNodes, badStats, 1000)
			ch.RecordSuccesses(t, badNodes, goodStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		// Start failing the bad nodes and keep giving the same stats to the good nodes.
		for i := 0; i < poolRefreshNo*2; i++ {
			badStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) * float64(10),
				Size:    float64(baseStatSize) / float64(10),
			}

			ch.RecordSuccesses(t, goodNodes, badStats, 1000)
			ch.RecordFailures(t, badNodes, 1000)
			ch.CaboosePool.DoRefresh()
		}

		ch.CaboosePool.DoRefresh()
		for n := range controlGroup {
			assert.Contains(t, ch.CabooseActiveNodes.Nodes, n)
		}

	})

}

func getHarnessAndControlGroup(t *testing.T, nodesSize int, poolSize int) (*util.CabooseHarness, map[string]string) {
	ch := util.BuildCabooseHarness(t, nodesSize, 3, func(config *caboose.Config) {
		config.PoolTargetSize = 3
	})

	ch.StartOrchestrator()

	rand.New(rand.NewSource(0))
	eps := ch.Endpoints
	controlGroup := make(map[string]string)

	rand.Shuffle(len(eps), func(i, j int) {
		eps[i], eps[j] = eps[j], eps[i]
	})

	for _, ep := range eps[:poolSize] {
		url, _ := url.Parse(ep.Server.URL)
		controlGroup[url.Host] = ep.Server.URL
	}

	return ch, controlGroup
}
