package caboose

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
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
	// TODO: Speed max bucket could use some further refinement,
	// for now we don't expect speed being  bigger than transfering 4MiB (max block) in 500ms
	// histogram buckets will be [1byte/milliseconds,  ... 8387 bytes/milliseconds] -> total 20 buckets +1 prometheus Inf bucket
	speedBytesPerMsHistogram = prometheus.ExponentialBucketsRange(1, 4194304/500, 20)

	// ----- Histogram buckets to record fetch duration metrics -----
	// The upper bound on the fetch duration buckets are informed by the timeouts per block and per peer request/retry.
	// buckets to record duration in milliseconds to fetch a CAR,
	// histogram buckets will be [50ms,.., 30 minutes] -> total 40 buckets +1 prometheus Inf bucket
	durationMsPerCarHistogram = prometheus.ExponentialBucketsRange(50, 1800000, 40)
)

var (
	fetchResponseCodeMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_response_code"),
		Help: "Response codes observed during caboose fetches",
	}, []string{"resourceType", "code"})

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

var (
	fetchCalledTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "fetch_called_total"),
	}, []string{"resourceType"})

	fetchRequestSuccessTimeTraceMetric = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_request_success_time_trace"),
		Buckets: durationMsPerCarHistogram,
	}, []string{"resourceType", "cache_status", "lifecycleStage"})
)

var (
	saturnCallsTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_total"),
	}, []string{"resourceType"})

	saturnCallsSuccessTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_success_total"),
	}, []string{"resourceType", "cache_status"})

	saturnCallsFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_calls_failure_total"),
	}, []string{"resourceType", "reason", "code"})

	saturnConnectionFailureTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "saturn_connection_failure_total"),
	}, []string{"resourceType", "reason"})

	mirroredTrafficTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "mirrored_traffic_total"),
	}, []string{"error_status"})

	complianceCidCallsTotalMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("ipfs", "caboose", "compliance_cids_total"),
	}, []string{"error_status"})
)

var CabooseMetrics = prometheus.NewRegistry()

func init() {
	// POOL Metrics
	CabooseMetrics.MustRegister(poolRefreshErrorMetric)
	CabooseMetrics.MustRegister(poolSizeMetric)
	CabooseMetrics.MustRegister(poolNewMembersMetric)
	CabooseMetrics.MustRegister(poolTierChangeMetric)

	CabooseMetrics.MustRegister(fetchResponseCodeMetric)
	CabooseMetrics.MustRegister(fetchSpeedPerPeerSuccessMetric)

	CabooseMetrics.MustRegister(fetchDurationPerCarPerPeerSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationPerCarPerPeerFailureMetric)
	CabooseMetrics.MustRegister(fetchDurationCarSuccessMetric)
	CabooseMetrics.MustRegister(fetchDurationCarFailureMetric)
	CabooseMetrics.MustRegister(fetchTTFBPerCARPerPeerSuccessMetric)

	CabooseMetrics.MustRegister(fetchCacheCountSuccessTotalMetric)

	CabooseMetrics.MustRegister(m_collector{&peerLatencyDistribution})

	CabooseMetrics.MustRegister(fetchSizeCarMetric)
	CabooseMetrics.MustRegister(fetchCalledTotalMetric)

	CabooseMetrics.MustRegister(saturnCallsTotalMetric)
	CabooseMetrics.MustRegister(saturnCallsFailureTotalMetric)
	CabooseMetrics.MustRegister(saturnConnectionFailureTotalMetric)
	CabooseMetrics.MustRegister(complianceCidCallsTotalMetric)

	CabooseMetrics.MustRegister(saturnCallsSuccessTotalMetric)

	CabooseMetrics.MustRegister(mirroredTrafficTotalMetric)

	CabooseMetrics.MustRegister(fetchRequestSuccessTimeTraceMetric)
}
