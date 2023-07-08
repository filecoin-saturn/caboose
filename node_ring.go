package caboose

import (
	"sync"

	"github.com/serialx/hashring"
)

// NodeRing represents a set of nodes organized for stable hashing.
type NodeRing struct {
	nodes map[string]*Node
	ring  hashring.HashRing

	lk sync.RWMutex
}

func NewNodeRing() *NodeRing {
	return &NodeRing{
		nodes: map[string]*Node{},
		ring:  *hashring.New([]string{}),
	}
}

func (nr *NodeRing) updateRing() error {
	// this method expects that the lk is held when called.
	rs := make(map[string]int)
	for _, n := range nr.nodes {
		// TODO: weight multiples
		rs[n.URL] = 1
	}
	nr.ring.UpdateWithWeights(rs)
	return nil
}

func (nr *NodeRing) Add(n *Node) error {
	nr.lk.Lock()
	defer nr.lk.Unlock()
	nr.nodes[n.URL] = n
	return nr.updateRing()
}

func (nr *NodeRing) Remove(n *Node) error {
	nr.lk.Lock()
	defer nr.lk.Unlock()

	if _, ok := nr.nodes[n.URL]; ok {
		delete(nr.nodes, n.URL)
		return nr.updateRing()
	}
	return ErrNoBackend
}

func (nr *NodeRing) Contains(n *Node) bool {
	nr.lk.RLock()
	defer nr.lk.RUnlock()

	_, ok := nr.nodes[n.URL]
	return ok
}

func (nr *NodeRing) GetNodes(key string, number int) ([]*Node, error) {
	nr.lk.RLock()
	defer nr.lk.RUnlock()

	keys, ok := nr.ring.GetNodes(key, number)
	if !ok {
		return nil, ErrNoBackend
	}
	nodes := make([]*Node, 0, len(keys))
	for _, k := range keys {
		if n, ok := nr.nodes[k]; ok {
			nodes = append(nodes, n)
		}
	}
	return nodes, nil
}
