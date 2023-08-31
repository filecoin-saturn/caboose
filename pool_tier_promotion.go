package caboose

const (
	poolConsiderationCount = 30
	activationThreshold    = 0
)

func updateActiveNodes(active *NodeRing, all *NodeHeap) error {
	candidates := all.TopN(poolConsiderationCount)
	added := 0
	for _, c := range candidates {
		if active.Contains(c) {
			continue
		}
		activeSize := active.Len()
		discount := poolConsiderationCount - activeSize
		if discount < 0 {
			discount = 0
		}
		thisThreshold := int64(activationThreshold - discount)
		add, err := active.MaybeSubstituteOrAdd(c, thisThreshold)
		if err != nil {
			return err
		}
		if add {
			added += 1
		}
	}
	return nil
}