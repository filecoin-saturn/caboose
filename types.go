package caboose

type responseMetrics struct {
	connFailure  bool
	networkError bool

	success  bool
	cacheHit bool

	respondeCode int
	ttfbMS       float64
	speedPerSec  float64
}
