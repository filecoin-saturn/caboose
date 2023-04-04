package caboose

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/tdigest"

	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	servertiming "github.com/mitchellh/go-server-timing"
)

var saturnReqTmpl = "/ipfs/%s?format=raw"

var (
	saturnNodeIdKey     = "Saturn-Node-Id"
	saturnTransferIdKey = "Saturn-Transfer-Id"
	saturnCacheHitKey   = "Saturn-Cache-Status"
	saturnCacheHit      = "HIT"
	saturnRetryAfterKey = "Retry-After"

	Mb = float64(1024 * 1024)
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

func (p *pool) fetchResource(ctx context.Context, from string, resource string, mime string, attempt int, cb DataCallback) (err error) {
	resourceType := "car"
	if mime == "application/vnd.ipld.raw" {
		resourceType = "block"
	}

	p.lk.Lock()
	if _, ok := p.nodesPerf[from]; !ok {
		p.nodesPerf[from] = &nodePerf{
			latencyDigest:    tdigest.NewWithCompression(1000),
			throughputDigest: tdigest.NewWithCompression(1000),
			nRequests:        uint64(1),
		}
	} else {
		p.nodesPerf[from].nRequests++
	}
	p.lk.Unlock()

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

			latencyMillis, speedBytesPerMs := updateSuccessServerTimingMetrics(respHeader.Values(servertiming.HeaderKey), resourceType, isCacheHit, durationMs, ttfbMs, received)

			if latencyMillis > 0 && speedBytesPerMs > 0 {
				p.lk.Lock()
				if _, ok := p.uniquePerf[from]; !ok {
					node := p.nodesPerf[from]
					node.nSuccess++
					node.latencyDigest.Add(latencyMillis, 1)
					node.throughputDigest.Add(speedBytesPerMs, 1)
					pct := (node.nSuccess * 100) / node.nRequests
					if pct >= 70 && node.nRequests >= 30 {
						p.uniquePerf[from] = struct{}{}
						fetchPeerLatencyDistributionMetric.WithLabelValues("P25").Observe(node.latencyDigest.Quantile(0.25))
						fetchPeerLatencyDistributionMetric.WithLabelValues("P50").Observe(node.latencyDigest.Quantile(0.50))
						fetchPeerLatencyDistributionMetric.WithLabelValues("P75").Observe(node.latencyDigest.Quantile(0.75))
						fetchPeerLatencyDistributionMetric.WithLabelValues("P90").Observe(node.latencyDigest.Quantile(0.90))
						fetchPeerLatencyDistributionMetric.WithLabelValues("P95").Observe(node.latencyDigest.Quantile(0.95))
						fetchPeerLatencyDistributionMetric.WithLabelValues("P99").Observe(node.latencyDigest.Quantile(0.99))

						fetchPeerSpeedDistributionMetric.WithLabelValues("P25").Observe(node.throughputDigest.Quantile(0.25))
						fetchPeerSpeedDistributionMetric.WithLabelValues("P50").Observe(node.throughputDigest.Quantile(0.50))
						fetchPeerSpeedDistributionMetric.WithLabelValues("P75").Observe(node.throughputDigest.Quantile(0.75))
						fetchPeerSpeedDistributionMetric.WithLabelValues("P90").Observe(node.throughputDigest.Quantile(0.90))
						fetchPeerSpeedDistributionMetric.WithLabelValues("P95").Observe(node.throughputDigest.Quantile(0.95))
						fetchPeerSpeedDistributionMetric.WithLabelValues("P99").Observe(node.throughputDigest.Quantile(0.99))
						// latency < 200ms
						if node.latencyDigest.Quantile(0.90) < 200 {
							fetchPeerP90GoodLatencyCountMetric.Add(1)
						}
						// > 1MB/s
						if (node.throughputDigest.Quantile(0.90) * 1000) > Mb {
							fetchPeerP90GoodSpeedCountMetric.Add(1)
						}
					}
				}
				p.lk.Unlock()
			}
		} else {
			if isBlockRequest {
				fetchDurationPerBlockPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			} else {
				fetchDurationPerCarPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			}
		}

		if received > 0 {
			fetchSizeMetric.WithLabelValues(resourceType).Observe(float64(received))
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

	var resp *http.Response
	resp, err = p.config.SaturnClient.Do(req)
	if err != nil {
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
		return
	}

	response_success_end = time.Now()
	return nil
}

// todo: refactor for dryness
func updateSuccessServerTimingMetrics(timingHeaders []string, resourceType string, isCacheHit bool, totalTimeMs, ttfbMs int64, recieved int) (latencyMillis, speedBytesPerMs float64) {
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
							fetchNetworkSpeedPerPeerSuccessMetric.WithLabelValues(resourceType).Observe(float64(recieved) / float64(networkTimeMs))
						}
						networkLatencyMs := ttfbMs - m.Duration.Milliseconds()
						fetchNetworkLatencyPeerSuccessMetric.WithLabelValues(resourceType).Observe(float64(networkLatencyMs))
						latencyMillis = float64(networkLatencyMs)
						speedBytesPerMs = (float64(recieved) / float64(networkTimeMs))
					}
				default:
				}
			}
		}
	}

	return
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
