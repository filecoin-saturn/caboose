package tieredhashing

type ResponseMetrics struct {
	ConnFailure  bool
	NetworkError bool
	ResponseCode int

	Success bool

	TTFBMs     float64
	SpeedPerMs float64
}
