package caboose

import (
	"math/rand"
	"testing"

	"github.com/filecoin-saturn/caboose/internal/state"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestPoolRefresh(t *testing.T) {
	p := newPool(&Config{}, nil)

	// Add 3 nodes
	nodes := []string{"node1", "node2", "node3"}
	addAndAssertPool(t, p, nodes, 3)

	// add no node
	addAndAssertPool(t, p, nil, 3)

	// add a new node
	addAndAssertPool(t, p, []string{"node4"}, 4)

	// add a new node with already added nodes
	addAndAssertPool(t, p, []string{"node1", "node2", "node3", "node4", "node5"}, 5)

	// record failure so that node is removed and then assert
	// TODO
	// removed node is NOT added back as pool is  full without it
	// TODO
}

func addAndAssertPool(t *testing.T, p *pool, nodes []string, expectedTotal int) {
	nodeStructs := genNodeStructs(nodes)
	for _, n := range nodeStructs {
		p.AllNodes.AddIfNotPresent(NewNode(n))
	}
	require.Equal(t, expectedTotal, p.AllNodes.Len())
}

func genNodeStructs(nodes []string) []state.NodeInfo {
	var nodeStructs []state.NodeInfo

	for _, node := range nodes {
		cid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte(node))
		nodeStructs = append(nodeStructs, state.NodeInfo{
			IP:            node,
			ID:            node,
			Weight:        rand.Intn(100),
			Distance:      rand.Float32(),
			ComplianceCid: cid.String(),
		})
	}
	return nodeStructs
}
