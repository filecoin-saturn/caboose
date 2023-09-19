package caboose_test

import (
	"context"
	cryptoRand "crypto/rand"
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
const blockPathPattern = "/ipfs/%s?format=car&dag-scope=block"

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
		for _, n := range ch.CabooseAllNodes.Nodes {
			_, ok := controlGroup[n.URL]
			if ok {
				goodNodes = append(goodNodes, n)
			}
		}

		for i := 0; i < 1; i++ {
			goodStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}

			ch.RecordSuccesses(t, goodNodes, goodStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		for n := range controlGroup {
			assert.Contains(t, ch.CabooseActiveNodes.Nodes, n)
		}
	})

	t.Run("pool converges to good nodes vs nodes with worse stats", func(t *testing.T) {
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

func TestPoolAffinity(t *testing.T) {
	baseStatSize := 100000
	baseStatLatency := 100
	// statVarianceFactor := 0.1
	poolRefreshNo := 10
	simReqCount := 10000
	ctx := context.Background()
	cidList := generateRandomCIDs(20)

	t.Run("selected nodes remain consistent for same cid reqs", func(t *testing.T) {
		ch, controlGroup := getHarnessAndControlGroup(t, nodesSize, nodesSize/2)
		_, _ = ch.Caboose.Get(ctx, cidList[0])

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

		// Send requests to control group nodes to bump their selection into the pool.
		for i := 0; i < poolRefreshNo; i++ {
			baseStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}

			ch.RecordSuccesses(t, goodNodes, baseStats, 1000)
			ch.CaboosePool.DoRefresh()
		}

		// Make a bunch of requests to similar cids to establish a stable hashring
		for i := 0; i < simReqCount; i++ {
			rand.New(rand.NewSource(time.Now().Unix()))
			idx := rand.Intn(len(cidList))
			_, _ = ch.Caboose.Get(ctx, cidList[idx])
		}
		ch.CaboosePool.DoRefresh()

		// Introduce new nodes by sendng same stats to those nodes.
		for i := 0; i < poolRefreshNo/2; i++ {
			baseStats := util.NodeStats{
				Start:   time.Now().Add(-time.Second * 2),
				Latency: float64(baseStatLatency) / float64(10),
				Size:    float64(baseStatSize) * float64(10),
			}

			// variedStats := util.NodeStats{
			// 	Start:   time.Now().Add(-time.Second * 2),
			// 	Latency: float64(baseStatLatency) / (float64(10) + (1 + statVarianceFactor)),
			// 	Size:    float64(baseStatSize) * float64(10) * (1 + statVarianceFactor),
			// }

			ch.RecordSuccesses(t, goodNodes, baseStats, 100)
			ch.RecordSuccesses(t, badNodes, baseStats, 10)

			ch.CaboosePool.DoRefresh()
		}

		// for _, i := range ch.CabooseAllNodes.Nodes {
		// 	fmt.Println(i.URL, i.Priority(), i.PredictedLatency)
		// }

		// Get the candidate nodes for a few cids from our formed cid list using
		// the affinity of each cid.
		for i := 0; i < 10; i++ {
			rand.New(rand.NewSource(time.Now().Unix()))
			idx := rand.Intn(len(cidList))
			c := cidList[idx]
			aff := ch.Caboose.GetAffinity(ctx)
			if aff == "" {
				aff = fmt.Sprintf(blockPathPattern, c)
			}
			nodes, _ := ch.CabooseActiveNodes.GetNodes(aff, ch.Config.MaxRetrievalAttempts)

			// We expect that the candidate nodes are part of the "good nodes" list.
			assert.Contains(t, goodNodes, nodes[0])
		}

	})
}

func getHarnessAndControlGroup(t *testing.T, nodesSize int, poolSize int) (*util.CabooseHarness, map[string]string) {
	ch := util.BuildCabooseHarness(t, nodesSize, 3, func(config *caboose.Config) {
		config.PoolTargetSize = nodesSize / 2
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

func generateRandomCIDs(count int) []cid.Cid {
	var cids []cid.Cid
	for i := 0; i < count; i++ {
		block := make([]byte, 32)
		cryptoRand.Read(block)
		c, _ := cid.V1Builder{
			Codec:  uint64(multicodec.Raw),
			MhType: uint64(multicodec.Sha2_256),
		}.Sum(block)

		cids = append(cids, c)
	}
	return cids
}
