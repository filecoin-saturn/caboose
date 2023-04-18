package tieredhashing

import "time"

type TieredHashingConfig struct {
	MaxPoolSizeNonEmpty int
	MaxPoolSizeEmpty    int
	AlwaysMainFirst     bool
	FailureDebounce     time.Duration
}

type Option func(config *TieredHashingConfig)

func WithAlwaysMainFirst() Option {
	return func(c *TieredHashingConfig) {
		c.AlwaysMainFirst = true
	}
}

func WithMaxPoolSizeNonEmpty(n int) Option {
	return func(c *TieredHashingConfig) {
		c.MaxPoolSizeNonEmpty = n
	}
}

func WithMaxPoolSizeEmpty(n int) Option {
	return func(c *TieredHashingConfig) {
		c.MaxPoolSizeEmpty = n
	}
}

func WithFailureDebounce(n time.Duration) Option {
	return func(c *TieredHashingConfig) {
		c.FailureDebounce = n
	}
}
