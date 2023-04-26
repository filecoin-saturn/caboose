package tieredhashing

import "time"

type TieredHashingConfig struct {
	MaxPoolSize     int
	AlwaysMainFirst bool
	FailureDebounce time.Duration
	WindowSize      int
	MaxMainTierSize int
}

type Option func(config *TieredHashingConfig)

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

func WithWindowSize(n int) Option {
	return func(c *TieredHashingConfig) {
		c.WindowSize = n
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
