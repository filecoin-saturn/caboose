package caboose

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/tdigest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	servertiming "github.com/mitchellh/go-server-timing"
	"github.com/tcnksm/go-httpstat"
)

var saturnReqTmpl = "/ipfs/%s?format=raw"

const (
	saturnNodeIdKey     = "Saturn-Node-Id"
	saturnTransferIdKey = "Saturn-Transfer-Id"
	saturnCacheHitKey   = "Saturn-Cache-Status"
	saturnCacheHit      = "HIT"
	saturnRetryAfterKey = "Retry-After"
	resourceTypeCar     = "car"
	resourceTypeBlock   = "block"
)

// doFetch attempts to fetch a block from a given Saturn endpoint. It sends the retrieval logs to the logging endpoint upon a successful or failed attempt.
func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid, attempt int) (b blocks.Block, e error) {
	reqUrl := fmt.Sprintf(saturnReqTmpl, c)

	e = p.fetchResource(ctx, from, reqUrl, "application/vnd.ipld.raw", attempt, func(rsrc string, r io.Reader) error {
		block, err := io.ReadAll(io.LimitReader(r, maxBlockSize))
		if err != nil {
			switch {
			case err == io.EOF && len(block) >= maxBlockSize:
				// we don't expect to see this error any time soon, but if IPFS
				// ecosystem ever starts allowing bigger blocks, this message will save
				// multiple people collective man-months in debugging ;-)
				return fmt.Errorf("strn responded with a block bigger than maxBlockSize=%d", maxBlockSize-1)
			case err == io.EOF:
				// This is fine :-)
				// Zero-length block may be valid (example: bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku)
				// We accept this as non-error and let it go over CID validation later.
			default:
				return fmt.Errorf("unable to read strn response body: %w", err)
			}
		}

		if p.config.DoValidation {
			nc, err := c.Prefix().Sum(block)
			if err != nil {
				return blocks.ErrWrongHash
			}
			if !nc.Equals(c) {
				return blocks.ErrWrongHash
			}
		}
		b, e = blocks.NewBlockWithCid(block, c)
		if e != nil {
			return e
		}
		return nil
	})
	return
}

// TODO Refactor to use a metrics collector that separates the collection of metrics from the actual fetching
func (p *pool) fetchResource(ctx context.Context, from string, resource string, mime string, attempt int, cb DataCallback) (err error) {
	resourceType := resourceTypeCar
	if mime == "application/vnd.ipld.raw" {
		resourceType = resourceTypeBlock
	}
	if ce := ctx.Err(); ce != nil {
		fetchRequestContextErrorTotalMetric.WithLabelValues(resourceType, fmt.Sprintf("%t", errors.Is(ce, context.Canceled)), "fetchResource-init").Add(1)
		return ce
	}

	ctx, span := spanTrace(ctx, "Pool.FetchResource", trace.WithAttributes(attribute.String("from", from), attribute.String("of", resource), attribute.String("mime", mime)))
	defer span.End()

	requestId := uuid.NewString()
	goLogger.Debugw("doing fetch", "from", from, "of", resource, "mime", mime, "requestId", requestId)

	start := time.Now()
	response_success_end := time.Now()

	fb := time.Unix(0, 0)
	code := 0
	proto := "unknown"
	respReq := &http.Request{}
	received := 0
	reqUrl := fmt.Sprintf("https://%s%s", from, resource)
	var respHeader http.Header
	saturnNodeId := ""
	saturnTransferId := ""
	isCacheHit := false
	networkError := ""

	isBlockRequest := false
	if mime == "application/vnd.ipld.raw" {
		isBlockRequest = true
	}

	// Get our timing header builder from the context
	timing := servertiming.FromContext(ctx)
	var timingMetric *servertiming.Metric
	if timing != nil {
		timingMetric = timing.NewMetric("fetch").Start()
		defer timingMetric.Stop()
	}
	var result httpstat.Result

	defer func() {
		if respHeader != nil {
			saturnNodeId = respHeader.Get(saturnNodeIdKey)
			saturnTransferId = respHeader.Get(saturnTransferIdKey)

			cacheHit := respHeader.Get(saturnCacheHitKey)
			if cacheHit == saturnCacheHit {
				isCacheHit = true
			}

			for k, v := range respHeader {
				received = received + len(k) + len(v)
			}
		}

		var ttfbMs int64
		durationSecs := time.Since(start).Seconds()
		durationMs := time.Since(start).Milliseconds()
		goLogger.Debugw("fetch result", "from", from, "of", resource, "status", code, "size", received, "ttfb", int(ttfbMs), "duration", durationSecs, "attempt", attempt, "error", err)

		fetchResponseCodeMetric.WithLabelValues(resourceType, fmt.Sprintf("%d", code)).Add(1)

		cacheStatus := getCacheStatus(isCacheHit)
		if err == nil && received > 0 {
			if isBlockRequest {
				fetchSizeBlockMetric.Observe(float64(received))
			} else {
				fetchSizeCarMetric.Observe(float64(received))
			}

			ttfbMs = fb.Sub(start).Milliseconds()
			fetchSpeedPerPeerSuccessMetric.WithLabelValues(resourceType, cacheStatus).Observe(float64(received) / float64(durationMs))
			fetchCacheCountSuccessTotalMetric.WithLabelValues(resourceType, cacheStatus).Add(1)
			// track individual block metrics separately
			if isBlockRequest {
				fetchTTFBPerBlockPerPeerSuccessMetric.WithLabelValues(cacheStatus).Observe(float64(ttfbMs))
				fetchDurationPerBlockPerPeerSuccessMetric.WithLabelValues(cacheStatus).Observe(float64(response_success_end.Sub(start).Milliseconds()))
			} else {
				fetchTTFBPerCARPerPeerSuccessMetric.WithLabelValues(cacheStatus).Observe(float64(ttfbMs))
				fetchDurationPerCarPerPeerSuccessMetric.WithLabelValues(cacheStatus).Observe(float64(response_success_end.Sub(start).Milliseconds()))
			}

			// update L1 server timings
			updateSuccessServerTimingMetrics(respHeader.Values(servertiming.HeaderKey), resourceType, isCacheHit, durationMs, ttfbMs, received)

			// record latency & speed dist for cache hits as we know that the observed fetch latency & speed correlates
			// with the network latency & speed here.
			if isCacheHit {
				p.lk.Lock()
				if _, ok := p.nodePerf[from]; !ok {
					p.nodePerf[from] = &perf{
						latencyDigest: tdigest.NewWithCompression(1000),
						speedDigest:   tdigest.NewWithCompression(1000),
					}
				}
				perf := p.nodePerf[from]
				perf.latencyDigest.Add(float64(ttfbMs), 1)
				perf.speedDigest.Add(float64(received)/float64(durationMs), 1)
				p.lk.Unlock()
			}
		} else {
			if isBlockRequest {
				fetchDurationPerBlockPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			} else {
				fetchDurationPerCarPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			}
		}

		p.logger.queue <- log{
			CacheHit:           isCacheHit,
			URL:                reqUrl,
			StartTime:          start,
			NumBytesSent:       received,
			RequestDurationSec: durationSecs,
			RequestID:          saturnTransferId,
			HTTPStatusCode:     code,
			HTTPProtocol:       proto,
			TTFBMS:             int(ttfbMs),
			// my address
			Range:          "",
			Referrer:       respReq.Referer(),
			UserAgent:      respReq.UserAgent(),
			NodeId:         saturnNodeId,
			NodeIpAddress:  from,
			IfNetworkError: networkError,
		}
	}()

	// TODO: Ideally, we would have additional "PerRequestInactivityTimeout"
	// which is the amount of time without any NEW data from the server, but
	// that can be added later. We need both because a slow trickle of data
	// could take a large amount of time.
	requestTimeout := DefaultSaturnCarRequestTimeout
	if isBlockRequest {
		requestTimeout = DefaultSaturnBlockRequestTimeout
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqUrl, nil)
	if err != nil {
		recordIfContextErr(resourceType, reqCtx, "build-http-request")
		return err
	}

	req.Header.Add("Accept", mime)
	if p.config.ExtraHeaders != nil {
		for k, vs := range *p.config.ExtraHeaders {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	//trace
	req = req.WithContext(httpstat.WithHTTPStat(req.Context(), &result))

	var resp *http.Response
	resp, err = p.config.SaturnClient.Do(req)
	if err != nil {
		recordIfContextErr(resourceType, reqCtx, "send-http-request")

		networkError = err.Error()
		return fmt.Errorf("http request failed: %w", err)
	}
	respHeader = resp.Header
	defer resp.Body.Close()

	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request

	if timing != nil {
		timingHeaders := respHeader.Values(servertiming.HeaderKey)
		for _, th := range timingHeaders {
			if subReqTiming, err := servertiming.ParseHeader(th); err == nil {
				for _, m := range subReqTiming.Metrics {
					m.Extra["attempt"] = fmt.Sprintf("%d", attempt)
					m.Extra["subreq"] = subReqID(from, resource)
					timing.Add(m)
				}
			}
		}
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			var retryAfter time.Duration
			if strnRetryHint := respHeader.Get(saturnRetryAfterKey); strnRetryHint != "" {
				seconds, err := strconv.ParseInt(strnRetryHint, 10, 64)
				if err == nil {
					retryAfter = time.Duration(seconds) * time.Second
				}
			}

			if retryAfter == 0 {
				retryAfter = p.config.SaturnNodeCoolOff
			}

			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, &ErrSaturnTooManyRequests{retryAfter: retryAfter, Node: from})
		}

		// empty body so it can be re-used.
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusGatewayTimeout {
			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrSaturnTimeout)
		}

		// This should only be 502, but L1s were not translating 404 from Lassie, so we have to support both for now.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadGateway {
			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrContentProviderNotFound)
		}

		return fmt.Errorf("http error from strn: %d", resp.StatusCode)
	}

	wrapped := TrackingReader{resp.Body, time.Time{}, 0}
	err = cb(resource, &wrapped)

	fb = wrapped.firstByte
	received = wrapped.len

	// drain body so it can be re-used.
	_, _ = io.Copy(io.Discard, resp.Body)
	if err != nil {
		recordIfContextErr(resourceType, reqCtx, "read-http-response")
		return
	}

	response_success_end = time.Now()

	// trace-metrics
	// request life-cycle metrics
	cacheHit := respHeader.Get(saturnCacheHitKey)
	if cacheHit == saturnCacheHit {
		isCacheHit = true
	}
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "tcp_connection").Observe(float64(result.TCPConnection.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "tls_handshake").Observe(float64(result.TLSHandshake.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "wait_after_request_sent_for_header").Observe(float64(result.ServerProcessing.Milliseconds()))

	return
}

func recordIfContextErr(resourceType string, ctx context.Context, requestState string) bool {
	if ce := ctx.Err(); ce != nil {
		fetchRequestContextErrorTotalMetric.WithLabelValues(resourceType, fmt.Sprintf("%t", errors.Is(ce, context.Canceled)), requestState).Add(1)
		return true
	}
	return false
}

// todo: refactor for dryness
func updateSuccessServerTimingMetrics(timingHeaders []string, resourceType string, isCacheHit bool, totalTimeMs, ttfbMs int64, recieved int) {
	if len(timingHeaders) == 0 {
		goLogger.Debug("no timing headers in request response.")
		return
	}

	for _, th := range timingHeaders {
		if subReqTiming, err := servertiming.ParseHeader(th); err == nil {
			for _, m := range subReqTiming.Metrics {
				switch m.Name {
				case "shim_lassie_headers":
					if m.Duration.Milliseconds() != 0 && !isCacheHit {
						fetchDurationPerPeerSuccessCacheMissTotalLassieMetric.WithLabelValues(resourceType).Observe(float64(m.Duration.Milliseconds()))
					}

				case "nginx":
					// sanity checks
					if totalTimeMs != 0 && ttfbMs != 0 && m.Duration.Milliseconds() != 0 {
						fetchDurationPerPeerSuccessTotalL1NodeMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit)).Observe(float64(m.Duration.Milliseconds()))
						networkTimeMs := totalTimeMs - m.Duration.Milliseconds()
						if networkTimeMs > 0 {
							s := float64(recieved) / float64(networkTimeMs)
							fetchNetworkSpeedPerPeerSuccessMetric.WithLabelValues(resourceType).Observe(s)
						}
						networkLatencyMs := ttfbMs - m.Duration.Milliseconds()
						fetchNetworkLatencyPeerSuccessMetric.WithLabelValues(resourceType).Observe(float64(networkLatencyMs))
					}
				default:
				}
			}
		}
	}
}

func getCacheStatus(isCacheHit bool) string {
	if isCacheHit {
		return "Cache-hit"
	}
	return "Cache-miss"
}

func subReqID(host, rsrc string) string {
	return fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(host+rsrc)))
}
