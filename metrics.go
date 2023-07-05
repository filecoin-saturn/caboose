package caboose

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// needed to sync over these global vars in tests
	distLk sync.Mutex

	peerLatencyDistribution prometheus.Collector // guarded by pool.lock
)

type m_collector struct {
	m *prometheus.Collector
}

func (mc m_collector) Describe(ch chan<- *prometheus.Desc) {
	if (*mc.m) != nil {
		(*mc.m).Describe(ch)
	}
}

func (mc m_collector) Collect(ch chan<- prometheus.Metric) {
	if (*mc.m) != nil {
		(*mc.m).Collect(ch)
	}
}

var (
	// size buckets from 256 KiB to ~8Gib
	// histogram buckets will be [256KiB, 512KiB, 1Mib, , ... 8GiB] -> total 16 buckets +1 prometheus Inf bucket
	carSizeHistogram = prometheus.ExponentialBuckets(256.0*1024, 2, 16)
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

	// buckets to record duration in milliseconds to fetch a block,
	// histogram buckets will be [50ms,.., 60 seconds] -> total 20 buckets +1 prometheus Inf bucket
	durationMsPerBlockHistogram = prometheus.ExponentialBucketsRange(50, 60000, 20)

	// buckets to record duration in milliseconds to fetch a CAR,
	// histogram buckets will be [50ms,.., 30 minutes] -> total 40 buckets +1 prometheus Inf bucket
	durationMsPerCarHistogram = prometheus.ExponentialBucketsRange(50, 1800000, 40)

	// buckets to measure latency between a caboose peer a Saturn L1,
	// [50ms, 100ms, 150ms, ...,  2000 ms]
	latencyDistMsHistogram = prometheus.LinearBuckets(50, 50, 40)
)

var (
	fetchResponseCodeMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_response_code"),
		Help: "Response codes observed during caboose fetches for a block",
	}, []string{"resourceType", "code", "range"})

	// success cases
	fetchSpeedPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_speed_peer_success"),
		Help:    "Speed observed during caboose fetches for successfully fetching from a single peer in bytes/ms",
		Buckets: speedBytesPerMsHistogram,
	}, []string{"resourceType", "cache_status"})

	fetchCacheCountSuccessTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_cache_count_success_total"),
		Help: "Records cache hits and cache hits for successful fetches from Saturn",
	}, []string{"resourceType", "cache_status"})
)

// block metrics
var (
	fetchDurationPerBlockPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_success"),
		Help:    "Latency observed during successful caboose fetches from a single peer in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	}, []string{"cache_status"})

	fetchDurationBlockSuccessMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_success"),
		Help:    "Latency observed during successful caboose fetches for a block across multiple peers and retries in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	})

	fetchTTFBPerBlockPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_ttfb_block_peer_success"),
		Help:    "TTFB observed during a successful caboose fetch from a single peer in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	}, []string{"cache_status"})

	// failures
	fetchDurationPerBlockPerPeerFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_peer_failure"),
		Help:    "Latency observed during failed caboose fetches from a single peer in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	})

	fetchDurationBlockFailureMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_block_failure"),
		Help:    "Latency observed during failed caboose fetches for a block across multiple peers and retries in milliseconds",
		Buckets: durationMsPerBlockHistogram,
	})

	fetchSizeBlockMetric = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size_block"),
		Help:    "Size in bytes of caboose block fetches",
		Buckets: blockSizeHistogram,
	})
)

// CAR metrics
var (
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
	}, []string{"cache_status", "car_size"})

	headerTTFBPerPeerMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "header_ttfb_peer"),
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType", "cache_status"})

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

	fetchSizeCarMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_size_car"),
		Help:    "Size in bytes of caboose CAR fetches",
		Buckets: carSizeHistogram,
	}, []string{"error_status"})
)

// Saturn Server-timings
var (
	// ---------------------- For successful fetches ONLY for now----------------------
	// L1 server timings
	// nginx + l1 compute + lassie
	fetchDurationPerPeerSuccessTotalL1NodeMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_peer_total_saturn_l1"),
		Help:    "Total time spent on an L1 node for a successful fetch per peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType", "cache_status"})

	// total only on lassie
	fetchDurationPerPeerSuccessCacheMissTotalLassieMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_peer_cache_miss_total_lassie"),
		Help:    "Time spent in Lassie for a Saturn L1 Nginx cache miss for a successful fetch per peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType"})

	lassie5XXTimeMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_duration_5xx_total_lassie"),
		Help:    "Time spent in Lassie for a Saturn L1 Nginx cache miss for a 5xx in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType"})

	// network timing
	fetchNetworkSpeedPerPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_network_speed_peer_success"),
		Help:    "Network speed observed during successful caboose fetches from a single peer in bytes per milliseconds",
		Buckets: speedBytesPerMsHistogram,
	}, []string{"resourceType"})

	fetchNetworkLatencyPeerSuccessMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_network_latency_peer_success"),
		Help:    "Network latency observed during successful caboose fetches from a single peer in milliseconds",
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType"})
)

var (
	fetchCalledTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_called_total"),
	}, []string{"resourceType"})

	fetchRequestContextErrorTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_request_context_error_total"),
	}, []string{"resourceType", "errorType", "requestStage"})

	fetchRequestSuccessTimeTraceMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_request_success_time_trace"),
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType", "cache_status", "lifecycleStage"})
)

var (
	saturnCallsTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_total"),
	}, []string{"resourceType", "range"})

	saturnCallsSuccessTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_success_total"),
	}, []string{"resourceType", "cache_status", "range"})

	saturnCalls2xxTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_2xx_total"),
	}, []string{"resourceType", "range"})

	saturnCallsFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_failure_total"),
	}, []string{"resourceType", "reason", "code", "range"})

	mirroredTrafficTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "mirrored_traffic_total"),
	}, []string{"error_status"})
)

var CabooseMetrics = prometheus.NewRegistry()

func init() {
	// POOL Metrics
	CabooseMetrics.MustRegister(poolRefreshErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolNewMembersMetric)
	CabooseMetrics.MustRegister(poolRemovedFailureTotalMetric)
	CabooseMetrics.MustRegister(poolRemovedConnFailureTotalMetric)
	CabooseMetrics.MustRegister(poolRemovedReadFailureTotalMetric)
	CabooseMetrics.MustRegister(poolRemovedNon2xxTotalMetric)
	CabooseMetrics.MustRegister(poolMembersNotAddedBecauseRemovedMetric)
	CabooseMetrics.MustRegister(poolMembersRemovedAndAddedBackMetric)
	CabooseMetrics.MustRegister(poolEnoughObservationsForMainSetDurationMetric)
	CabooseMetrics.MustRegister(poolTierChangeMetric)

	CabooseMetrics.MustRegister(fetchResponseCodeMetric)
	CabooseMetrics.MustRegister(fetchSpeedPerPeerSuccessMetric)

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
	CabooseMetrics.MustRegister(headerTTFBPerPeerMetric)

	CabooseMetrics.MustRegister(fetchCacheCountSuccessTotalMetric)

	CabooseMetrics.MustRegister(fetchDurationPerPeerSuccessTotalL1NodeMetric)
	CabooseMetrics.MustRegister(fetchDurationPerPeerSuccessCacheMissTotalLassieMetric)
	CabooseMetrics.MustRegister(lassie5XXTimeMetric)

	CabooseMetrics.MustRegister(fetchNetworkSpeedPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchNetworkLatencyPeerSuccessMetric)

	CabooseMetrics.MustRegister(m_collector{&peerLatencyDistribution})

	CabooseMetrics.MustRegister(fetchSizeCarMetric)
	CabooseMetrics.MustRegister(fetchSizeBlockMetric)

	CabooseMetrics.MustRegister(fetchRequestContextErrorTotalMetric)
	CabooseMetrics.MustRegister(fetchCalledTotalMetric)
	CabooseMetrics.MustRegister(fetchRequestSuccessTimeTraceMetric)

	CabooseMetrics.MustRegister(saturnCallsTotalMetric)
	CabooseMetrics.MustRegister(saturnCallsFailureTotalMetric)
	CabooseMetrics.MustRegister(saturnCalls2xxTotalMetric)

	CabooseMetrics.MustRegister(saturnCallsSuccessTotalMetric)

	CabooseMetrics.MustRegister(mirroredTrafficTotalMetric)
}
