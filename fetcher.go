package caboose

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"
	servertiming "github.com/mitchellh/go-server-timing"
	"github.com/tcnksm/go-httpstat"
)

const (
	UserAgentTag = "STRN_ENV_TAG"

	saturnNodeIdKey     = "Saturn-Node-Id"
	saturnTransferIdKey = "Saturn-Transfer-Id"
	saturnCacheHitKey   = "Saturn-Cache-Status"
	saturnCacheHit      = "HIT"
	saturnRetryAfterKey = "Retry-After"
	resourceTypeCar     = "car"
	resourceTypeBlock   = "block"
)

var (
	// 256 Kib to 8Gib
	carSizes = []float64{262144, 524288, 1.048576e+06, 2.097152e+06, 4.194304e+06,
		8.388608e+06, 1.6777216e+07, 3.3554432e+07, 6.7108864e+07, 1.34217728e+08,
		2.68435456e+08, 5.36870912e+08, 1.073741824e+09, 2.147483648e+09, 4.294967296e+09, 8.589934592e+09}

	carSizesStr = []string{"256.0 Kib", "512.0 Kib", "1.0 Mib", "2.0 Mib",
		"4.0 Mib", "8.0 Mib", "16.0 Mib", "32.0 Mib", "64.0 Mib", "128.0 Mib", "256.0 Mib", "512.0 Mib", "1.0 Gib", "2.0 Gib", "4.0 Gib", "8.0 Gib"}
)

// TODO Refactor to use a metrics collector that separates the collection of metrics from the actual fetching
func (p *pool) fetchResource(ctx context.Context, from *Node, resource string, mime string, attempt int, cb DataCallback) (err error) {
	resourceType := resourceTypeCar
	if mime == "application/vnd.ipld.raw" {
		resourceType = resourceTypeBlock
	}
	// if the context is already cancelled, there's nothing we can do here.
	if ce := ctx.Err(); ce != nil {
		return ce
	}

	ctx, span := spanTrace(ctx, "Pool.FetchResource", trace.WithAttributes(attribute.String("from", from.URL), attribute.String("of", resource), attribute.String("mime", mime)))
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
	reqUrl := fmt.Sprintf("https://%s%s", from.URL, resource)
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
		// do nothing if this is because of a cancelled context
		if err != nil && errors.Is(err, context.Canceled) {
			return
		}
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

		durationSecs := time.Since(start).Seconds()
		goLogger.Debugw("fetch result", "from", from, "of", resource, "status", code, "size", received, "duration", durationSecs, "attempt", attempt, "error", err,
			"proto", proto)
		fetchResponseCodeMetric.WithLabelValues(resourceType, fmt.Sprintf("%d", code)).Add(1)
		var ttfbMs int64

		if err == nil && received > 0 {
			ttfbMs = fb.Sub(start).Milliseconds()

			cacheStatus := getCacheStatus(isCacheHit)
			if !isBlockRequest {
				fetchSizeCarMetric.WithLabelValues("success").Observe(float64(received))
			}
			durationMs := response_success_end.Sub(start).Milliseconds()
			fetchSpeedPerPeerSuccessMetric.WithLabelValues(resourceType, cacheStatus).Observe(float64(received) / float64(durationMs))
			fetchCacheCountSuccessTotalMetric.WithLabelValues(resourceType, cacheStatus).Add(1)
			// track individual block metrics separately
			if !isBlockRequest {
				ci := 0
				for index, value := range carSizes {
					if float64(received) < value {
						ci = index
						break
					}
				}
				carSizeStr := carSizesStr[ci]

				fetchTTFBPerCARPerPeerSuccessMetric.WithLabelValues(cacheStatus, carSizeStr).Observe(float64(ttfbMs))
				fetchDurationPerCarPerPeerSuccessMetric.WithLabelValues(cacheStatus).Observe(float64(response_success_end.Sub(start).Milliseconds()))
			}
		} else {
			if !isBlockRequest {
				fetchDurationPerCarPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
				fetchSizeCarMetric.WithLabelValues("failure").Observe(float64(received))
			}
		}

		if err == nil || !errors.Is(err, context.Canceled) {
			if p.logger != nil {
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
					NodeIpAddress:  from.URL,
					IfNetworkError: networkError,
				}
			}
		}
	}()

	// TODO: Ideally, we would have additional "PerRequestInactivityTimeout"
	// which is the amount of time without any NEW data from the server, but
	// that can be added later. We need both because a slow trickle of data
	// could take a large amount of time.
	requestTimeout := DefaultCarRequestTimeout
	if isBlockRequest {
		requestTimeout = DefaultBlockRequestTimeout
	}

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqUrl, nil)
	if err != nil {
		if isCtxError(reqCtx) {
			return reqCtx.Err()
		}
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

	agent := req.Header.Get("User-Agent")
	req.Header.Set("User-Agent", os.Getenv(UserAgentTag)+"/"+agent)

	//trace
	req = req.WithContext(httpstat.WithHTTPStat(req.Context(), &result))

	var resp *http.Response
	saturnCallsTotalMetric.WithLabelValues(resourceType).Add(1)

	resp, err = p.config.Client.Do(req)
	if err != nil {
		if isCtxError(reqCtx) {
			if errors.Is(err, context.Canceled) {
				return reqCtx.Err()
			}
		}

		if errors.Is(err, context.DeadlineExceeded) {
			saturnConnectionFailureTotalMetric.WithLabelValues(resourceType, "timeout").Add(1)
		} else {
			saturnConnectionFailureTotalMetric.WithLabelValues(resourceType, "non-timeout").Add(1)
		}
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
					m.Extra["subreq"] = subReqID(from.URL, resource)
					timing.Add(m)
				}
			}
		}
	}

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)

		saturnCallsFailureTotalMetric.WithLabelValues(resourceType, "non-2xx", fmt.Sprintf("%d", resp.StatusCode)).Add(1)
		if resp.StatusCode == http.StatusTooManyRequests {
			var retryAfter time.Duration
			if strnRetryHint := respHeader.Get(saturnRetryAfterKey); strnRetryHint != "" {
				seconds, err := strconv.ParseInt(strnRetryHint, 10, 64)
				if err == nil {
					retryAfter = time.Duration(seconds) * time.Second
				}
			}

			if retryAfter == 0 {
				retryAfter = p.config.CoolOff
			}

			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, &ErrTooManyRequests{retryAfter: retryAfter, Node: from.URL})
		}

		// empty body so it can be re-used.
		if resp.StatusCode == http.StatusGatewayTimeout {
			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrTimeout)
		}

		// This should only be 502, but L1s were not translating 404 from Lassie, so we have to support both for now.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadGateway {
			return fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrContentProviderNotFound)
		}

		return fmt.Errorf("http error from strn: %d", resp.StatusCode)
	}
	if respHeader.Get(saturnCacheHitKey) == saturnCacheHit {
		isCacheHit = true
	}

	wrapped := TrackingReader{resp.Body, time.Time{}, 0}
	err = cb(resource, &wrapped)
	received = wrapped.len
	// drain body so it can be re-used.
	_, _ = io.Copy(io.Discard, resp.Body)

	if err != nil {
		if isCtxError(reqCtx) {
			if errors.Is(err, context.Canceled) {
				return reqCtx.Err()
			}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			saturnCallsFailureTotalMetric.WithLabelValues(resourceType, fmt.Sprintf("failed-response-read-timeout-%s", getCacheStatus(isCacheHit)),
				fmt.Sprintf("%d", code)).Add(1)
		} else {
			saturnCallsFailureTotalMetric.WithLabelValues(resourceType, fmt.Sprintf("failed-response-read-%s", getCacheStatus(isCacheHit)), fmt.Sprintf("%d", code)).Add(1)
		}

		networkError = err.Error()
		return err
	}

	fb = wrapped.firstByte
	response_success_end = time.Now()

	// trace-metrics
	// request life-cycle metrics
	saturnCallsSuccessTotalMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit)).Add(1)

	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "tcp_connection").Observe(float64(result.TCPConnection.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "tls_handshake").Observe(float64(result.TLSHandshake.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "wait_after_request_sent_for_header").Observe(float64(result.ServerProcessing.Milliseconds()))

	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "name_lookup").
		Observe(float64(result.NameLookup.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "connect").
		Observe(float64(result.Connect.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "pre_transfer").
		Observe(float64(result.Pretransfer.Milliseconds()))
	fetchRequestSuccessTimeTraceMetric.WithLabelValues(resourceType, getCacheStatus(isCacheHit), "start_transfer").
		Observe(float64(result.StartTransfer.Milliseconds()))

	from.RecordSuccess(start, float64(wrapped.firstByte.Sub(start).Milliseconds()), float64(received)/float64(response_success_end.Sub(start).Milliseconds()))

	return nil
}

func isCtxError(ctx context.Context) bool {
	if ce := ctx.Err(); ce != nil {
		return true
	}
	return false
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
