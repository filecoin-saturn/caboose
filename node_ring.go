package caboose

import (
	"fmt"
	"strings"
	"sync"

	"github.com/willscott/hashring"
)

// NodeRing represents a set of nodes organized for stable hashing.
type NodeRing struct {
	Nodes      map[string]*Node
	ring       hashring.HashRing
	targetSize int

	lk sync.RWMutex
}

func NewNodeRing(targetSize int) *NodeRing {
	return &NodeRing{
		Nodes:      map[string]*Node{},
		ring:       *hashring.New([]string{}),
		targetSize: targetSize,
	}
}

func (nr *NodeRing) updateRing() error {
	// this method expects that the lk is held when called.
	rs := make(map[string]int)
	for _, n := range nr.Nodes {
		// TODO: weight multiples
		rs[n.URL] = 1
	}
	nr.ring.UpdateWithWeights(rs)
	return nil
}

// A score of '0' ==> overall experience is the same as the current state
// A positive score ==> overall experience is better than the current state
// A negative score ==> overall experience is worse than the current state
func (nr *NodeRing) getScoreForUpdate(candidate string, priority float64, weight int) float64 {
	changes := nr.ring.ConsiderUpdateWeightedNode(candidate, weight)
	delta := float64(0)
	var neighbor *Node

	for n, v := range changes {
		neighbor = nr.Nodes[n]
		neighborVolume := neighbor.Rate()
		if neighborVolume < 1 {
			neighborVolume = 1
		}

		amntChanged := v
		// for now, add some bounds
		if amntChanged < -1 {
			amntChanged = -1
		} else if amntChanged > 1 {
			amntChanged = 1
		}
		// a negative amntChanged means that we're replacing the neighbor with the candidate.
		amntChanged *= -1

		// how much worse is candidate?
		diff := priority - neighbor.Priority()
		cs := diff * neighborVolume * float64(amntChanged)
		delta += cs
		// fmt.Printf("+%f (n %s: diff %f=(n %f - candidate %f) * volume %f * v = %f)", cs, neighbor.URL, diff, neighbor.Priority(), priority, neighborVolume, amntChanged)
	}
	return delta
}

func (nr *NodeRing) MaybeSubstituteOrAdd(candidate *Node, activationThreshold int64) (bool, error) {
	nr.lk.Lock()
	defer nr.lk.Unlock()

	_, ok := nr.ring.GetNode(candidate.URL)
	if !ok {
		// ring is empty. in this case we always want to add.
		nr.Nodes[candidate.URL] = candidate
		return true, nr.updateRing()
	}

	// how much space is being claimed?
	delta := nr.getScoreForUpdate(candidate.URL, candidate.Priority(), 1)

	if delta >= float64(activationThreshold) {
		nr.Nodes[candidate.URL] = candidate
		return true, nr.updateRing()
	}

	// not a clear benefit to add, but maybe acceptable for substitution:
	worst := candidate.Priority()
	worstN := ""
	for _, n := range nr.Nodes {
		if n.Priority() < worst {
			worst = n.Priority()
			worstN = n.URL
		}
	}

	// todo: the '+1' is an arbitrary threshold to prevent thrashing. it should be configurable.
	if worstN != "" && candidate.Priority()-worst > float64(activationThreshold)+1 {
		nr.Nodes[candidate.URL] = candidate
		delete(nr.Nodes, worstN)
		return true, nr.updateRing()

	}

	// fmt.Printf("did not add - delta %f activation %d, node priority %f\n", delta, activationThreshold, candidate.Priority())
	return false, nil
}

func (nr *NodeRing) Add(n *Node) error {
	nr.lk.Lock()
	defer nr.lk.Unlock()
	nr.Nodes[n.URL] = n
	return nr.updateRing()
}

func (nr *NodeRing) Remove(n *Node) error {
	nr.lk.Lock()
	defer nr.lk.Unlock()

	if _, ok := nr.Nodes[n.URL]; ok {
		delete(nr.Nodes, n.URL)
		return nr.updateRing()
	}
	return ErrNoBackend
}

func (nr *NodeRing) Contains(n *Node) bool {
	nr.lk.RLock()
	defer nr.lk.RUnlock()

	_, ok := nr.Nodes[n.URL]
	return ok
}

func (nr *NodeRing) GetNodes(key string, number int) ([]*Node, error) {
	nr.lk.RLock()
	defer nr.lk.RUnlock()

	if number > nr.ring.Size() {
		number = nr.ring.Size()
	}
	keys, ok := nr.ring.GetNodes(key, number)
	if !ok {
		return nil, ErrNoBackend
	}
	nodes := make([]*Node, 0, len(keys))
	for _, k := range keys {
		if n, ok := nr.Nodes[k]; ok {
			nodes = append(nodes, n)
		}
	}
	return nodes, nil
}

func (nr *NodeRing) Len() int {
	nr.lk.RLock()
	defer nr.lk.RUnlock()
	return nr.ring.Size()
}

func (nr *NodeRing) String() string {
	nr.lk.RLock()
	defer nr.lk.RUnlock()

	ns := make([]string, 0, len(nr.Nodes))
	for _, n := range nr.Nodes {
		ns = append(ns, n.String())
	}

	return fmt.Sprintf("NodeRing[len %d]{%s}", nr.ring.Size(), strings.Join(ns, ","))
}
