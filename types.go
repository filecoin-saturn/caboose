package caboose

type responseMetrics struct {
	reqBuildError bool
	connFailure   bool
	networkError  bool
	responseCode  int

	success  bool
	cacheHit bool

	ttfbMS      float64
	speedPerSec float64
}
