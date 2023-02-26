package caboose

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (

	// Size buckets from 256 KiB (default chunk in Kubo) to 4MiB (maxBlockSize), 256 KiB  wide each
	blockSizeHistogram = prometheus.LinearBuckets(262144, 262144, 16)

	// TODO: Speed max bucket could use some further refinement,
	// for now we don't expect speed being  bigger than transfering 4MiB (max block) in 500ms
	speedHistogram = prometheus.ExponentialBucketsRange(1, 4194304/500, 20)

	// Duration max bucket is informed by the timeouts per block and per peer request/retry
	durationPerBlockHistogram        = prometheus.ExponentialBucketsRange(50, 60000, 10)
	durationPerBlockPerPeerHistogram = prometheus.ExponentialBucketsRange(50, 20000, 10)

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

	// TODO: if we add CARs, we need to split this one into two, or add two dedicated ones
	fetchResponseMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_errors"),
		Help: "Errors fetching from Caboose Peers",
	}, []string{"code"})

	fetchSpeedPerBlockMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_block"),
		Help:    "Speed observed during caboose fetches for a block across multiple peers and retries",
		Buckets: speedHistogram,
	})

	fetchSpeedPerBlockPerPeerMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_block_peer"),
		Help:    "Speed observed during caboose fetches for fetching a block from a single peer",
		Buckets: speedHistogram,
	})

	// TODO: if we add CARs, we need to split this one into two, or add two dedicated ones
	fetchSizeMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size"),
		Help:    "Size in bytes of caboose block fetches",
		Buckets: blockSizeHistogram,
	})

	fetchDurationPerBlockPerPeerSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_success"),
		Help:    "Latency observed during successful caboose fetches from a single peer",
		Buckets: durationPerBlockPerPeerHistogram,
	})

	fetchDurationPerBlockPerPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_failure"),
		Help:    "Latency observed during failed caboose fetches from a single peer",
		Buckets: durationPerBlockPerPeerHistogram,
	})

	fetchDurationBlockSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_success"),
		Help:    "Latency observed during successful caboose fetches for a block",
		Buckets: durationPerBlockHistogram,
	})

	fetchDurationCarSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_success"),
		Help:    "Latency observed during successful caboose fetches for a car",
		Buckets: durationPerBlockHistogram,
	})

	fetchDurationBlockFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_failure"),
		Help:    "Latency observed during failed caboose fetches for a block",
		Buckets: durationPerBlockHistogram,
	})

	fetchDurationCarFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_failure"),
		Help:    "Latency observed during failed caboose fetches for a car",
		Buckets: durationPerBlockHistogram,
	})

	fetchTTFBPerBlockPerPeerSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_block_peer_success"),
		Help:    "TTFB observed during a successful caboose fetch from a single peer",
		Buckets: durationPerBlockPerPeerHistogram,
	})

	fetchTTFBPerBlockPerPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_block_peer_failure"),
		Help:    "TTFB observed during a failed caboose fetch from a single peer",
		Buckets: durationPerBlockPerPeerHistogram,
	})
)

func init() {
	CabooseMetrics.MustRegister(poolErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolHealthMetric)
	CabooseMetrics.MustRegister(fetchResponseMetric)
	CabooseMetrics.MustRegister(fetchSizeMetric)

	CabooseMetrics.MustRegister(fetchSpeedPerBlockMetric)
	CabooseMetrics.MustRegister(fetchSpeedPerBlockPerPeerMetric)
	CabooseMetrics.MustRegister(fetchDurationPerBlockPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationPerBlockPerPeerFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationCarSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationCarFailureMetric)
	CabooseMetrics.MustRegister(fetchTTFBPerBlockPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchTTFBPerBlockPerPeerFailureMetric)
}
