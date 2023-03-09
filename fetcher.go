package caboose

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	blocks "github.com/ipfs/go-libipfs/blocks"
)

var saturnReqTmpl = "/ipfs/%s?format=raw"

var (
	saturnNodeIdKey     = "Saturn-Node-Id"
	saturnTransferIdKey = "Saturn-Transfer-Id"
	saturnCacheHitKey   = "Saturn-Cache-Status"
	saturnCacheHit      = "HIT"
	saturnRetryAfterKey = "Retry-After"
)

// doFetch attempts to fetch a block from a given Saturn endpoint. It sends the retrieval logs to the logging endpoint upon a successful or failed attempt.
func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid, attempt int) (b blocks.Block, fs float64, e error) {
	reqUrl := fmt.Sprintf(saturnReqTmpl, c)

	fs, e = p.fetchResource(ctx, from, reqUrl, "application/vnd.ipld.raw", attempt, func(rsrc string, r io.Reader) error {
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

func (p *pool) fetchResource(ctx context.Context, from string, resource string, mime string, attempt int, cb DataCallback) (fetchSpeed float64, err error) {
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

	defer func() {
		var ttfbMs int64
		durationSecs := time.Since(start).Seconds()
		durationMs := time.Since(start).Milliseconds()
		goLogger.Debugw("fetch result", "from", from, "of", resource, "status", code, "size", received, "ttfb", int(ttfbMs), "duration", durationSecs, "attempt", attempt, "error", err)
		fetchResponseMetric.WithLabelValues(fmt.Sprintf("%d", code)).Add(1)

		if err == nil && received > 0 {
			ttfbMs = fb.Sub(start).Milliseconds()
			if ttfbMs < 0 {
				goLogger.Errorw("negative ttfb", "from", from, "of", resource, "ttfb", ttfbMs, "start", start, "fb", fb, "err", err,
					"recieved", received)
			}
			fetchTTFBPerBlockPerPeerSuccessMetric.Observe(float64(ttfbMs))
			// track individual block metrics separately
			if mime == "application/vnd.ipld.raw" {
				fetchDurationPerBlockPerPeerSuccessMetric.Observe(float64(response_success_end.Sub(start).Milliseconds()))
			} else {
				fetchDurationPerCarPerPeerSuccessMetric.Observe(float64(response_success_end.Sub(start).Milliseconds()))
			}
			fetchSpeedPerBlockPerPeerMetric.Observe(float64(received) / float64(durationMs))
		} else {
			fetchTTFBPerBlockPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			if mime == "application/vnd.ipld.raw" {
				fetchDurationPerBlockPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			} else {
				fetchDurationPerCarPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
			}
		}

		if received > 0 {
			fetchSizeMetric.Observe(float64(received))
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

	reqCtx, cancel := context.WithTimeout(ctx, DefaultSaturnRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqUrl, nil)
	if err != nil {
		return 0, err
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
		return 0, fmt.Errorf("http request failed: %w", err)
	}
	respHeader = resp.Header
	defer resp.Body.Close()

	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			var retryAfter time.Duration
			if strnRetryHint := resp.Header.Get(saturnRetryAfterKey); strnRetryHint != "" {
				seconds, err := strconv.ParseInt(strnRetryHint, 10, 64)
				if err == nil {
					retryAfter = time.Duration(seconds) * time.Second
				}
			}

			if retryAfter == 0 {
				retryAfter = p.config.SaturnNodeCoolOff
			}

			return 0, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, &ErrSaturnTooManyRequests{retryAfter: retryAfter, Node: from})
		}

		// empty body so it can be re-used.
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusGatewayTimeout {
			return 0, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrSaturnTimeout)
		}

		// This should only be 502, but L1s were not translating 404 from Lassie, so we have to support both for now.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadGateway {
			return 0, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrContentProviderNotFound)
		}

		return 0, fmt.Errorf("http error from strn: %d", resp.StatusCode)
	}

	wrapped := TrackingReader{resp.Body, time.Time{}, 0}
	err = cb(resource, &wrapped)
	fetch_end := time.Now()

	fb = wrapped.firstByte
	received = wrapped.len

	// drain body so it can be re-used.
	_, _ = io.Copy(io.Discard, resp.Body)

	if err != nil {
		return 0, err
	}

	response_success_end = time.Now()
	return float64(float64(received) / float64(fetch_end.Sub(start).Milliseconds())), nil
}
