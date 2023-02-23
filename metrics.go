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
		Help: "Health of the caboose pool",
	}, []string{"weight"})

	fetchResponseMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_errors"),
		Help: "Errors fetching from Caboose Peers",
	}, []string{"code"})

	fetchSpeedPerBlockMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_block"),
		Help:    "Speed observed during caboose fetches for a block across multiple peers",
		Buckets: prometheus.ExponentialBucketsRange(1, maxBlockSize/500, 20),
	})

	fetchSpeedPerPeerMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_peer"),
		Help:    "Speed observed during caboose fetches for fetching a block from a single peer",
		Buckets: prometheus.ExponentialBucketsRange(1, maxBlockSize/500, 20),
	})

	fetchSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size"),
		Help:    "Size in bytes of caboose fetches",
		Buckets: prometheus.ExponentialBucketsRange(1, maxBlockSize, 16),
	})

	fetchDurationPeerSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_peer_success"),
		Help:    "Latency observed during successful caboose fetches from a single peer",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnRequestTimeout.Milliseconds()), 10),
	})

	fetchDurationPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_peer_failure"),
		Help:    "Latency observed during failed caboose fetches from a single peer",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnRequestTimeout.Milliseconds()), 10),
	})

	fetchDurationBlockSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_success"),
		Help:    "Latency observed during successful caboose fetches for a block",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnGlobalBlockFetchTimeout.Milliseconds()), 10),
	})

	fetchDurationBlockFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_failure"),
		Help:    "Latency observed during failed caboose fetches for a block",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnGlobalBlockFetchTimeout.Milliseconds()), 10),
	})

	fetchTTFBPeerSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_peer_success"),
		Help:    "TTFB observed during a successful caboose fetch from a single peer",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnRequestTimeout.Milliseconds()), 10),
	})

	fetchTTFBPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_peer_failure"),
		Help:    "TTFB observed during a failed caboose fetch from a single peer",
		Buckets: prometheus.ExponentialBucketsRange(1, float64(DefaultSaturnRequestTimeout.Milliseconds()), 10),
	})
)

func init() {
	CabooseMetrics.MustRegister(poolErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolHealthMetric)
	CabooseMetrics.MustRegister(fetchResponseMetric)
	CabooseMetrics.MustRegister(fetchSizeMetric)

	CabooseMetrics.MustRegister(fetchSpeedPerBlockMetric)
	CabooseMetrics.MustRegister(fetchSpeedPerPeerMetric)
	CabooseMetrics.MustRegister(fetchDurationPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationPeerFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockFailureMetric)
	CabooseMetrics.MustRegister(fetchTTFBPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchTTFBPeerFailureMetric)
}
