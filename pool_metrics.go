package caboose

import "github.com/prometheus/client_golang/prometheus"

// pool metrics
var (
	poolRemovedFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_removed_failure_total"),
	}, []string{"tier", "reason"})

	poolRemovedConnFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_removed_conn_failure_total"),
	}, []string{"tier"})

	poolRemovedReadFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_removed_read_failure_total"),
	}, []string{"tier"})

	poolRemovedNon2xxTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_removed_non2xx_total"),
	}, []string{"tier"})

	// The below metrics are only updated periodically on every Caboose pool refresh
	poolMembersNotAddedBecauseRemovedMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_members_not_added"),
	})

	poolRefreshErrorMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_refresh_errors"),
		Help: "Number of errors refreshing the caboose pool",
	})

	poolSizeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_size"),
		Help: "Number of active caboose peers",
	}, []string{"tier"})

	poolNewMembersMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_new_members"),
		Help: "New members added to the Caboose pool",
	})

	poolEnoughObservationsForMainSetDurationMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_observations_for_main_set_duration"),
	})

	poolTierChangeMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_tier_change"),
	}, []string{"change"})
)
