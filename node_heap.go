package caboose

import (
	"container/heap"
	"math/rand"
	"sync"
)

// NodeHeap is a collection of nodes organized by performance
type NodeHeap struct {
	Nodes []*Node
	lk    sync.RWMutex
}

func NewNodeHeap() *NodeHeap {
	return &NodeHeap{Nodes: make([]*Node, 0)}
}

func (nh *NodeHeap) Add(n *Node) {
	nh.lk.Lock()
	defer nh.lk.Unlock()
	heap.Push(nh, n)
}

func (nh *NodeHeap) AddIfNotPresent(n *Node) bool {
	nh.lk.Lock()
	defer nh.lk.Unlock()

	for _, e := range nh.Nodes {
		if e.Equals(n) {
			return false
		}
	}
	heap.Push(nh, n)
	return true
}

func (nh *NodeHeap) Best() *Node {
	nh.lk.Lock()
	defer nh.lk.Unlock()
	heap.Init(nh)
	item := heap.Pop(nh)
	return item.(*Node)
}

func (nh *NodeHeap) PeekRandom() *Node {
	nh.lk.RLock()
	defer nh.lk.RUnlock()

	if len(nh.Nodes) == 0 {
		return nil
	}

	randIdx := rand.Intn(len(nh.Nodes))
	return nh.Nodes[randIdx]
}

func (nh *NodeHeap) TopN(n int) []*Node {
	m := make([]*Node, 0, n)
	temp := make([]*Node, 0, n)
	nh.lk.Lock()
	defer nh.lk.Unlock()

	heap.Init(nh)
	for i := 0; i < n && nh.Len() > 0; i++ {
		item := heap.Pop(nh)
		node := item.(*Node)
		m = append(m, node)
		temp = append(temp, node)
	}

	for _, node := range temp {
		heap.Push(nh, node)
	}

	return m
}

/* below functions implement the heap interface */
var _ heap.Interface = (*NodeHeap)(nil)

func (nh *NodeHeap) Len() int { return len(nh.Nodes) }

func (nh *NodeHeap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return nh.Nodes[i].Priority() > nh.Nodes[j].Priority()
}

func (nh *NodeHeap) Swap(i, j int) {
	nh.Nodes[i], nh.Nodes[j] = nh.Nodes[j], nh.Nodes[i]
}

func (nh *NodeHeap) Push(a any) {
	if n, ok := a.(*Node); ok {
		nh.Nodes = append(nh.Nodes, n)
	}
}

func (nh *NodeHeap) Pop() any {
	n := len(nh.Nodes)
	item := nh.Nodes[n-1]
	nh.Nodes[n-1] = nil
	nh.Nodes = nh.Nodes[0 : n-1]
	return item
}
