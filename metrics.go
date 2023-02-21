package caboose

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CabooseMetrics = prometheus.NewRegistry()

	poolErrorMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_errors"),
		Help: "Number of errors refreshing the caboose pool",
	})

	poolSizeMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_size"),
		Help: "Number of active caboose peers",
	})

	poolHealthMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_health"),
		Help: "Helath of the caboose pool",
	}, []string{"weight"})

	fetchResponseMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_errors"),
		Help: "Errors fetching from Caboose Peers",
	}, []string{"code"})

	fetchSpeedMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed"),
		Help:    "Speed observed during caboose fetches",
		Buckets: prometheus.DefBuckets,
	})
	fetchLatencyMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_latency"),
		Help:    "Latency observed during caboose fetches",
		Buckets: prometheus.ExponentialBucketsRange(1, 10000, 10),
	})
	fetchSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size"),
		Help:    "Size in bytes of caboose fetches",
		Buckets: prometheus.ExponentialBucketsRange(1, 4000000, 16),
	})
)

func init() {
	CabooseMetrics.MustRegister(poolErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolHealthMetric)
	CabooseMetrics.MustRegister(fetchResponseMetric)
	CabooseMetrics.MustRegister(fetchSpeedMetric)
	CabooseMetrics.MustRegister(fetchLatencyMetric)
	CabooseMetrics.MustRegister(fetchSizeMetric)
}
