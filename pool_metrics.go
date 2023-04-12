package caboose

import "github.com/prometheus/client_golang/prometheus"

// pool metrics
var (
	poolRemovedCorrectnessTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_removed_correctness_total"),
	}, []string{"tier"})

	poolRefreshErrorMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_refresh_errors"),
		Help: "Number of errors refreshing the caboose pool",
	})

	// The below metrics are only updated periodically on every Caboose pool refresh
	poolSizeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_size"),
		Help: "Number of active caboose peers",
	}, []string{"tier"})

	poolNewMembersMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_new_members"),
		Help: "New members added to the Caboose pool",
	})
)
