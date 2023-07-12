package caboose

const (
	poolConsiderationCount = 30
	activationThreshold    = 10.0
)

func updateActiveNodes(active *NodeRing, all *NodeHeap) error {
	candidates := all.TopN(poolConsiderationCount)
	for _, c := range candidates {
		if active.Contains(c) {
			continue
		}
		if err := active.MaybeSubstituteOrAdd(c, activationThreshold); err != nil {
			return err
		}
	}
	return nil
}
