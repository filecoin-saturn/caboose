package caboose

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// size buckets from 256 KiB (default chunk in Kubo) to 4MiB (maxBlockSize), 256 KiB  wide each
	// histogram buckets will be [256KiB, 512KiB, 768KiB, 1MiB, ... 4MiB] -> total 16 buckets +1 prometheus Inf bucket
	blockSizeHistogram = prometheus.LinearBuckets(262144, 262144, 16)

	// TODO: Speed max bucket could use some further refinement,
	// for now we don't expect speed being  bigger than transfering 4MiB (max block) in 500ms
	// histogram buckets will be [1byte/milliseconds,  ... 8387 bytes/milliseconds] -> total 20 buckets +1 prometheus Inf bucket
	speedBytesPerMsHistogram = prometheus.ExponentialBucketsRange(1, 4194304/500, 20)

	// ----- Histogram buckets to record fetch duration metrics -----
	// The upper bound on the fetch duration buckets are informed by the timeouts per block and per peer request/retry.

	// buckets to record duration in milliseconds to fetch a block across multiple peers,
	// histogram buckets will be [50ms,.., 60 seconds] -> total 10 buckets +1 prometheus Inf bucket
	durationMsPerBlockHistogram = prometheus.ExponentialBucketsRange(50, 60000, 10)

	// buckets to record duration in milliseconds to fetch a block from a single peer,
	// histogram buckets will be [50ms,.., 20 seconds] -> total 10 buckets +1 prometheus Inf bucket
	durationMsPerBlockPerPeerHistogram = prometheus.ExponentialBucketsRange(50, 20000, 10)

	// buckets to record duration in milliseconds to fetch a CAR,
	// histogram buckets will be [50ms,.., 30 minutes] -> total 10 buckets +1 prometheus Inf bucket
	durationMsPerCarHistogram = prometheus.ExponentialBucketsRange(50, 1800000, 10)
)

// pool metrics
var (
	poolErrorMetric = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_errors"),
		Help: "Number of errors refreshing the caboose pool",
	})

	// The below metrics are only updated periodically on every Caboose pool refresh
	poolSizeMetric = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_size"),
		Help: "Number of active caboose peers",
	})

	poolHealthMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_health"),
		Help: "Health of the caboose pool",
	}, []string{"weight"})

	poolNewMembersMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "pool_new_members"),
		Help: "New members added to the Caboose pool",
	}, []string{"weight"})
)

var (
	fetchResponseCodeMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_response_code"),
		Help: "Response codes observed during caboose fetches for a block",
	}, []string{"resourceType", "code"})

	fetchSizeMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size"),
		Help:    "Size in bytes of caboose block fetches",
		Buckets: blockSizeHistogram,
	}, []string{"resourceType"})
)

// block metrics
var (
	// success cases
	fetchSpeedPerBlockMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_block"),
		Help:    "Speed observed during caboose fetches for a block across multiple peers and retries in bytes/ms",
		Buckets: speedBytesPerMsHistogram,
	})

	fetchSpeedPerBlockPerPeerMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_block_peer"),
		Help:    "Speed observed during caboose fetches for fetching a block from a single peer in bytes/ms",
		Buckets: speedBytesPerMsHistogram,
	}, []string{"cache_status"})

	fetchDurationPerBlockPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_success"),
		Help:    "Latency observed during successful caboose fetches from a single peer in milliseconds",
		Buckets: durationMsPerBlockPerPeerHistogram,
	}, []string{"cache_status"})

	fetchDurationBlockSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_success"),
		Help:    "Latency observed during successful caboose fetches for a block across multiple peers and retries in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	})

	fetchTTFBPerBlockPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_block_peer_success"),
		Help:    "TTFB observed during a successful caboose fetch from a single peer in milliseconds",
		Buckets: durationMsPerBlockPerPeerHistogram,
	}, []string{"cache_status"})

	// failures
	fetchDurationPerBlockPerPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_failure"),
		Help:    "Latency observed during failed caboose fetches from a single peer in milliseconds",
		Buckets: durationMsPerBlockPerPeerHistogram,
	})

	fetchDurationBlockFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_failure"),
		Help:    "Latency observed during failed caboose fetches for a block across multiple peers and retries in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	})
)

// CAR metrics
var (
	// success
	fetchSpeedPerCarPerPeerMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_car_peer"),
		Help:    "Speed observed during caboose fetches for fetching a car from a single peer in bytes/ms",
		Buckets: speedBytesPerMsHistogram,
	}, []string{"cache_status"})

	fetchDurationPerCarPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_peer_success"),
		Help:    "Latency observed during successful caboose car fetches from a single peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"cache_status"})

	fetchDurationCarSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_success"),
		Help:    "Latency observed during successful caboose fetches for a car across multiple peers and retries in milliseconds",
		Buckets: durationMsPerCarHistogram,
	})

	fetchTTFBPerCARPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_car_peer_success"),
		Help:    "TTFB observed during a successful caboose CAR fetch from a single peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"cache_status"})

	// failure
	fetchDurationPerCarPerPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_peer_failure"),
		Help:    "Latency observed during failed caboose car fetches from a single peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	})

	fetchDurationCarFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_car_failure"),
		Help:    "Latency observed during failed caboose fetches for a car across multiple peers and retries in milliseconds",
		Buckets: durationMsPerCarHistogram,
	})
)

var CabooseMetrics = prometheus.NewRegistry()

func init() {
	CabooseMetrics.MustRegister(poolErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolHealthMetric)
	CabooseMetrics.MustRegister(poolNewMembersMetric)

	CabooseMetrics.MustRegister(fetchResponseCodeMetric)
	CabooseMetrics.MustRegister(fetchSizeMetric)

	CabooseMetrics.MustRegister(fetchSpeedPerBlockMetric)
	CabooseMetrics.MustRegister(fetchSpeedPerBlockPerPeerMetric)

	CabooseMetrics.MustRegister(fetchSpeedPerCarPerPeerMetric)

	CabooseMetrics.MustRegister(fetchDurationPerBlockPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationPerCarPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationPerBlockPerPeerFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationPerCarPerPeerFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationCarSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationBlockFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationCarFailureMetric)
	CabooseMetrics.MustRegister(fetchTTFBPerBlockPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchTTFBPerCARPerPeerSuccessMetric)
}
