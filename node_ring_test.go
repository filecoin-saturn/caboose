package caboose_test

import (
	"fmt"
	"testing"

	"github.com/filecoin-saturn/caboose"
)

func TestNodeRing(t *testing.T) {
	nr := caboose.NewNodeRing()
	nodes := make([]caboose.Node, 0)
	for i := 0; i < 100; i++ {
		nodes = append(nodes, caboose.Node{URL: fmt.Sprintf("node%d", i)})
	}

	for _, n := range nodes {
		err := nr.Add(&n)
		if err != nil {
			t.Fatalf("adding should always work: %v", err)
		}
	}
}
