package tieredhashing

import (
	"testing"

	"github.com/asecurityteam/rolling"

	"github.com/stretchr/testify/require"
)

func TestNodesSortedLatency(t *testing.T) {
	window := 3

	tcs := map[string]struct {
		expected []nodeWithLatency
		initFn   func(*TieredHashing)
	}{
		"no nodes": {
			expected: nil,
		},
		"one node with not enough observations": {
			expected: nil,
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				for i := 0; i < window-1; i++ {
					h.nodes["node1"].LatencyDigest.Append(1)
					h.nodes["node1"].NLatencyDigest++
				}
			},
		},
		"two nodes with not enough observations": {
			expected: nil,
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				h.nodes["node2"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

			},
		},
		"one node with enough observations": {
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				for i := 0; i < window; i++ {
					h.nodes["node1"].LatencyDigest.Append(1)
					h.nodes["node1"].NLatencyDigest++
				}
			},
			expected: []nodeWithLatency{{node: "node1", latency: 1}},
		},
		"two nodes with enough observations": {
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}
				h.nodes["node2"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				for i := 0; i < window; i++ {
					h.nodes["node1"].LatencyDigest.Append(2)
					h.nodes["node1"].NLatencyDigest++

					h.nodes["node2"].LatencyDigest.Append(1)
					h.nodes["node2"].NLatencyDigest++
				}
			},
			expected: []nodeWithLatency{{node: "node2", latency: 1}, {node: "node1", latency: 2}},
		},
		"3 nodes; 2 have enough observations": {
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}
				h.nodes["node2"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				h.nodes["node3"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				for i := 0; i < window; i++ {
					h.nodes["node2"].LatencyDigest.Append(20)
					h.nodes["node2"].NLatencyDigest++

					h.nodes["node3"].LatencyDigest.Append(10)
					h.nodes["node3"].NLatencyDigest++

					if i != window-1 {
						h.nodes["node1"].LatencyDigest.Append(3)
						h.nodes["node1"].NLatencyDigest++
					}
				}
			},
			expected: []nodeWithLatency{{node: "node3", latency: 10}, {node: "node2", latency: 20}},
		},
		"rolling window": {
			initFn: func(h *TieredHashing) {
				h.nodes["node1"] = &NodePerf{
					Tier:          TierUnknown,
					LatencyDigest: rolling.NewPointPolicy(rolling.NewWindow(window)),
				}

				for i := 0; i < window+10; i++ {
					h.nodes["node1"].LatencyDigest.Append(float64(i))
					h.nodes["node1"].NLatencyDigest++
				}
			},
			expected: []nodeWithLatency{{node: "node1", latency: 12}},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			th := NewTieredHashingHarness(WithLatencyWindowSize(window))

			if tc.initFn != nil {
				tc.initFn(th.h)
			}

			nds := th.h.nodesSortedLatency()
			require.EqualValues(t, tc.expected, nds)
		})
	}
}
