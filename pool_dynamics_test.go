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
)


const (
	nodesSize = 10
	nodesPoolSize = caboose.PoolConsiderationCount
)


func TestPoolDynamics(t *testing.T) {

	ch := util.BuildCabooseHarness(t, nodesSize , 3)
	ch.StartOrchestrator()
	baseStatSize := 100
	baseStatLatency := 100
	ctx := context.Background()

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)

	ch.FetchAndAssertSuccess(t, ctx, testCid)

	rand.New(rand.NewSource(0))
	eps := ch.Endpoints
	controlGroup := make(map[string]string)

	rand.Shuffle(len(eps), func(i, j int) {
		eps[i], eps[j] = eps[j], eps[i]
	})

	for _,ep := range(eps[:nodesPoolSize]) {
		url, _ := url.Parse(ep.Server.URL)
		controlGroup[url.Host] = ep.Server.URL

	}

	for i := 0; i < 1; i++ {
		nodes := ch.CabooseAllNodes
		goodNodes := make([]*caboose.Node, 0)
		badNodes := make([]*caboose.Node, 0)
		goodStats := util.NodeStats{
			Start: time.Now().Add(-time.Second*6),
			Latency: float64(baseStatLatency) / float64(10),
			Size: float64(baseStatSize) * float64(10),
		}
		badStats := util.NodeStats{
			Start: time.Now().Add(-time.Second*6),
			Latency: float64(baseStatLatency) * float64(10),
			Size: float64(baseStatSize) / float64(10),
		}
		for _,n := range(nodes.Nodes) {
			_, ok := controlGroup[n.URL]
			if ok {
				fmt.Println("Good", n.URL)

				goodNodes = append(goodNodes, n)
			} else {
				fmt.Println("Bad", n.URL)

				badNodes = append(badNodes, n)
			}
		}

		ch.RecordSuccesses(t, goodNodes, goodStats, 10)
		ch.RecordSuccesses(t, badNodes, badStats, 10)

	}


	ch.Caboose.Pool.DoRefresh()

	fmt.Println("Pool", ch.CabooseActiveNodes.Nodes)

	for _,n := range(ch.CabooseAllNodes.Nodes) {
		fmt.Println("Node", n.URL, "size", n.PredictedThroughput)
	}

}
