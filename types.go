package caboose

type responseMetrics struct {
	reqBuildError bool
	connFailure   bool
	networkError  bool
	responseCode  int
	isConnTimeout bool
	isReadTimeout bool

	success  bool
	cacheHit bool

	ttfbMS     float64
	speedPerMs float64
}
