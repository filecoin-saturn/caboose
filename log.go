package caboose

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

type logger struct {
	queue    chan log
	freq     time.Duration
	client   *http.Client
	endpoint url.URL
	done     chan struct{}
}

func newLogger(c *Config) *logger {
	l := logger{
		queue:    make(chan log, 5),
		freq:     c.LoggingInterval,
		client:   c.LoggingClient,
		endpoint: c.LoggingEndpoint,
		done:     make(chan struct{}),
	}
	go l.background()
	return &l
}

func (l *logger) background() {
	t := time.NewTimer(l.freq)
	pending := make([]log, 0, 100)
	for {
		select {
		case l := <-l.queue:
			pending = append(pending, l)
		case <-t.C:
			if len(pending) > 0 {
				//submit.
				toSubmit := make([]log, len(pending))
				copy(toSubmit, pending)
				pending = pending[:0]
				go l.submit(toSubmit)
			}
			t.Reset(l.freq)
		case <-l.done:
			return
		}
	}
}

func (l *logger) submit(logs []log) {
	finalLogs := bytes.NewBuffer(nil)
	enc := json.NewEncoder(finalLogs)
	enc.Encode(logBatch{logs})
	l.client.Post(l.endpoint.String(), "application/json", finalLogs)
}

func (l *logger) Close() {
	close(l.done)
}

type logBatch struct {
	Logs []log `json:"bandwidthLogs"`
}

type log struct {
	CacheHit        bool      `json:"cacheHit"`
	URL             string    `json:"url"`
	LocalTime       time.Time `json:"localTime"`
	NumBytesSent    int       `json:"numBytesSent"`
	RequestDuration float64   `json:"requestDuration"` // in seconds
	RequestID       string    `json:"requestId"`
	HTTPStatusCode  int       `json:"httpStatusCode"`
	HTTPProtocol    string    `json:"httpProtocol"`
	TTFBMS          int       `json:"ttfbMs"`
	ClientAddress   string    `json:"clientAddress"`
	Range           string    `json:"range"`
	Referrer        string    `json:"referrer"`
	UserAgent       string    `json:"userAgent"`
}
