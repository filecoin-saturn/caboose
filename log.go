package caboose

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	golog "github.com/ipfs/go-log/v2"
)

// goLogger gets process aggregation logs
var goLogger = golog.Logger("caboose")

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

	req, err := http.NewRequest(http.MethodPost, l.endpoint.String(), finalLogs)
	if err != nil {
		goLogger.Errorw("failed to create http request to submit saturn logs", "err", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		goLogger.Errorw("failed to submit saturn logs", "err", err)
		return
	}
	if resp.StatusCode/100 != 2 {
		goLogger.Errorw("saturn logging endpoint did not return 2xx", "status", resp.StatusCode)
	} else {
		goLogger.Debugw("saturn logging endpoint returned 2xx")
	}
}

func (l *logger) Close() {
	close(l.done)
}

type logBatch struct {
	Logs []log `json:"bandwidthLogs"`
}

type log struct {
	CacheHit           bool      `json:"cacheHit"`
	URL                string    `json:"url"`
	StartTime          time.Time `json:"startTime"`
	NumBytesSent       int       `json:"numBytesSent"`
	RequestDurationSec float64   `json:"requestDurationSec"` // in seconds
	RequestID          string    `json:"requestId"`
	HTTPStatusCode     int       `json:"httpStatusCode"`
	HTTPProtocol       string    `json:"httpProtocol"`
	TTFBMS             int       `json:"ttfbMs"`
	Range              string    `json:"range"`
	Referrer           string    `json:"referrer"`
	UserAgent          string    `json:"userAgent"`
	NodeId             string    `json:"nodeId"`
	IfNetworkError     string    `json:"ifNetworkError"`
	NodeIpAddress      string    `json:"nodeIpAddress"`
	VerificationError  string    `json:"verificationError"`
	OtherError         string    `json:"otherError"`
}
