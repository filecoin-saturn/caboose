package tieredhashing

import "time"

type TieredHashingConfig struct {
	MaxPoolSize           int
	AlwaysMainFirst       bool
	FailureDebounce       time.Duration
	LatencyWindowSize     int
	CorrectnessWindowSize int
	MaxMainTierSize       int
	NoRemove              bool
	CorrectnessPct        float64
}

type Option func(config *TieredHashingConfig)

func WithNoRemove(noRemove bool) Option {
	return func(c *TieredHashingConfig) {
		c.NoRemove = noRemove
	}
}

func WithAlwaysMainFirst() Option {
	return func(c *TieredHashingConfig) {
		c.AlwaysMainFirst = true
	}
}

func WithMaxMainTierSize(n int) Option {
	return func(c *TieredHashingConfig) {
		c.MaxMainTierSize = n
	}
}

func WithCorrectnessPct(pct float64) Option {
	return func(c *TieredHashingConfig) {
		c.CorrectnessPct = pct
	}
}

func WithCorrectnessWindowSize(n int) Option {
	return func(c *TieredHashingConfig) {
		c.CorrectnessWindowSize = n
	}
}

func WithLatencyWindowSize(n int) Option {
	return func(c *TieredHashingConfig) {
		c.LatencyWindowSize = n
	}
}

func WithMaxPoolSize(n int) Option {
	return func(c *TieredHashingConfig) {
		c.MaxPoolSize = n
	}
}

func WithFailureDebounce(n time.Duration) Option {
	return func(c *TieredHashingConfig) {
		c.FailureDebounce = n
	}
}
