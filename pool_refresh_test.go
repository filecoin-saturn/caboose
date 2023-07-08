package caboose

import (
	"testing"

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
	for _, n := range nodes {
		p.AllNodes.AddIfNotPresent(NewNode(n))
	}
	require.Equal(t, expectedTotal, p.AllNodes.Len())
}
