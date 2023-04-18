package tieredhashing

type ResponseMetrics struct {
	ConnFailure  bool
	NetworkError bool
	ResponseCode int

	Success  bool
	CacheHit bool

	TTFBMs     float64
	SpeedPerMs float64
}
