package caboose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/asecurityteam/rolling"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/filecoin-saturn/caboose/tieredhashing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/patrickmn/go-cache"
)

const (
	tierMainToUnknown = "main-to-unknown"
	tierUnknownToMain = "unknown-to-main"
)

// loadPool refreshes the set of Saturn endpoints in the pool by fetching an updated list of responsive Saturn nodes from the
// Saturn Orchestrator.
func (p *pool) loadPool() ([]string, error) {
	if override := os.Getenv("CABOOSE_BACKEND_OVERRIDE"); len(override) > 0 {
		return strings.Split(override, ","), nil
	}
	resp, err := p.config.OrchestratorClient.Get(p.config.OrchestratorEndpoint.String())
	if err != nil {
		goLogger.Warnw("failed to get backends from orchestrator", "err", err, "endpoint", p.config.OrchestratorEndpoint.String())
		return nil, err
	}
	defer resp.Body.Close()

	responses := make([]string, 0)
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		goLogger.Warnw("failed to decode backends from orchestrator", "err", err, "endpoint", p.config.OrchestratorEndpoint.String())
		return nil, err
	}
	goLogger.Infow("got backends from orchestrators", "cnt", len(responses), "endpoint", p.config.OrchestratorEndpoint.String())
	return responses, nil
}

type pool struct {
	config *Config
	logger *logger

	started chan struct{} // started signals that we've already initialized the pool once with Saturn endpoints.
	refresh chan struct{} // refresh is used to signal the need for doing a refresh of the Saturn endpoints pool.
	done    chan struct{} // done is used to signal that we're shutting down the Saturn endpoints pool and don't need to refresh it anymore.

	fetchKeyLk            sync.RWMutex
	fetchKeyFailureCache  *cache.Cache // guarded by fetchKeyLk
	fetchKeyCoolDownCache *cache.Cache // guarded by fetchKeyLk

	lk sync.RWMutex
	th *tieredhashing.TieredHashing

	poolInitDone sync.Once
}

func newPool(c *Config) *pool {
	p := pool{
		config:  c,
		started: make(chan struct{}),
		refresh: make(chan struct{}, 1),
		done:    make(chan struct{}, 1),

		fetchKeyCoolDownCache: cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),
		fetchKeyFailureCache:  cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),
		th:                    tieredhashing.New(c.TieredHashingOpts...),
	}

	return &p
}

func (p *pool) Start() {
	go p.refreshPool()
}

func (p *pool) doRefresh() {
	newEP, err := p.loadPool()
	if err == nil {
		p.refreshWithNodes(newEP)
	} else {
		poolRefreshErrorMetric.Add(1)
	}
}

func (p *pool) refreshWithNodes(newEP []string) {
	p.lk.Lock()
	defer p.lk.Unlock()

	// for tests to pass the -race check when accessing global vars
	distLk.Lock()
	defer distLk.Unlock()

	added, alreadyRemoved := p.th.AddOrchestratorNodes(newEP)
	poolNewMembersMetric.Set(float64(added))
	poolMembersNotAddedBecauseRemovedMetric.Set(float64(alreadyRemoved))

	// update the tier set
	mu, um := p.th.UpdateMainTierWithTopN()
	poolTierChangeMetric.WithLabelValues(tierMainToUnknown).Set(float64(mu))
	poolTierChangeMetric.WithLabelValues(tierUnknownToMain).Set(float64(um))

	mt := p.th.GetPoolMetrics()
	poolSizeMetric.WithLabelValues(string(tieredhashing.TierUnknown)).Set(float64(mt.Unknown))
	poolSizeMetric.WithLabelValues(string(tieredhashing.TierMain)).Set(float64(mt.Main))

	// Update aggregate latency & speed distribution for peers
	latencyHist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName("ipfs", "caboose", "fetch_peer_latency_dist"),
		Help:    "Fetch latency distribution for peers in millis",
		Buckets: latencyDistMsHistogram,
	}, []string{"tier", "percentile"})

	percentiles := []float64{25, 50, 75, 90, 95}

	for _, perf := range p.th.GetPerf() {
		perf := perf
		if perf.NLatencyDigest <= 0 {
			continue
		}

		for _, pt := range percentiles {
			latencyHist.WithLabelValues(string(perf.Tier), fmt.Sprintf("P%f", pt)).Observe(perf.LatencyDigest.Reduce(rolling.Percentile(pt)))
		}
	}
	peerLatencyDistribution = latencyHist
}

func (p *pool) refreshPool() {
	t := time.NewTimer(0)
	started := sync.Once{}
	for {
		select {
		case <-t.C:
			p.doRefresh()
			started.Do(func() {
				close(p.started)
			})

			t.Reset(p.config.PoolRefresh)
		case <-p.refresh:
			p.doRefresh()
			started.Do(func() {
				close(p.started)
			})

			if !t.Stop() {
				<-t.C
			}
			t.Reset(p.config.PoolRefresh)
		case <-p.done:
			return
		}
	}
}

func (p *pool) Close() {
	select {
	case p.done <- struct{}{}:
		return
	default:
		return
	}
}

func cidToKey(c cid.Cid) string {
	return c.Hash().B58String()
}

func (p *pool) fetchBlockWith(ctx context.Context, c cid.Cid, with string) (blk blocks.Block, err error) {
	fetchCalledTotalMetric.WithLabelValues(resourceTypeBlock).Add(1)
	if recordIfContextErr(resourceTypeBlock, ctx, "fetchBlockWith") {
		return nil, ctx.Err()
	}
	// wait for pool to be initialised
	<-p.started

	// if the cid is in the cool down cache, we fail the request.
	p.fetchKeyLk.RLock()
	if at, ok := p.fetchKeyCoolDownCache.Get(cidToKey(c)); ok {
		p.fetchKeyLk.RUnlock()

		expireAt := at.(time.Time)
		return nil, &ErrCoolDown{
			Cid:        c,
			retryAfter: time.Until(expireAt),
		}
	}
	p.fetchKeyLk.RUnlock()

	aff := with
	if aff == "" {
		aff = cidToKey(c)
	}

	p.lk.RLock()
	nodes := p.th.GetNodes(tieredhashing.TierMain, aff, p.config.MaxRetrievalAttempts)
	if len(nodes) < p.config.MaxRetrievalAttempts {
		nodes = append(nodes,
			p.th.GetNodes(tieredhashing.TierUnknown, aff, p.config.MaxRetrievalAttempts-len(nodes))...,
		)
	}
	p.lk.RUnlock()
	if len(nodes) < p.config.MaxRetrievalAttempts {
		return nil, ErrNoBackend
	}

	blockFetchStart := time.Now()
	for i := 0; i < len(nodes); i++ {
		if recordIfContextErr(resourceTypeBlock, ctx, "fetchBlockWithLoop") {
			return nil, ctx.Err()
		}
		blk, err = p.fetchBlockAndUpdate(ctx, nodes[i], c, i)
		if err != nil && errors.Is(err, context.Canceled) {
			return nil, err
		}

		if err == nil {
			durationMs := time.Since(blockFetchStart).Milliseconds()
			fetchDurationBlockSuccessMetric.Observe(float64(durationMs))
			return
		}
	}

	fetchDurationBlockFailureMetric.Observe(float64(time.Since(blockFetchStart).Milliseconds()))

	p.updateFetchKeyCoolDown(cidToKey(c))

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

// record the failure in the cid failure cache and
// if the number of cid fetch failures has crossed a certain threshold, add the cid to a cool down cache.
func (p *pool) updateFetchKeyCoolDown(key string) {
	p.fetchKeyLk.Lock()
	defer p.fetchKeyLk.Unlock()

	expireAt := time.Now().Add(p.config.FetchKeyCoolDownDuration)

	if p.config.MaxFetchFailuresBeforeCoolDown == 1 {
		p.fetchKeyCoolDownCache.Set(key, expireAt, time.Until(expireAt)+100)
		return
	}

	v, ok := p.fetchKeyFailureCache.Get(key)
	if !ok {
		p.fetchKeyFailureCache.Set(key, 1, cache.DefaultExpiration)
		return
	}

	count := v.(int)
	if p.config.MaxFetchFailuresBeforeCoolDown == 1 || count+1 == p.config.MaxFetchFailuresBeforeCoolDown {
		p.fetchKeyCoolDownCache.Set(key, expireAt, time.Until(expireAt)+100)
		p.fetchKeyFailureCache.Delete(key)
	} else {
		p.fetchKeyFailureCache.Set(key, count+1, cache.DefaultExpiration)
	}
}

func (p *pool) fetchResourceWith(ctx context.Context, path string, cb DataCallback, with string) (err error) {
	fetchCalledTotalMetric.WithLabelValues(resourceTypeCar).Add(1)
	if recordIfContextErr(resourceTypeCar, ctx, "fetchResourceWith") {
		return ctx.Err()
	}

	// wait for pool to be initialised
	<-p.started

	// if the cid is in the cool down cache, we fail the request.
	p.fetchKeyLk.RLock()
	if at, ok := p.fetchKeyCoolDownCache.Get(path); ok {
		p.fetchKeyLk.RUnlock()

		expireAt := at.(time.Time)
		return &ErrCoolDown{
			Path:       path,
			retryAfter: time.Until(expireAt),
		}
	}
	p.fetchKeyLk.RUnlock()

	aff := with
	if aff == "" {
		aff = path
	}

	p.lk.RLock()
	nodes := p.th.GetNodes(tieredhashing.TierMain, aff, p.config.MaxRetrievalAttempts)
	if len(nodes) < p.config.MaxRetrievalAttempts {
		nodes = append(nodes,
			p.th.GetNodes(tieredhashing.TierUnknown, aff, p.config.MaxRetrievalAttempts-len(nodes))...,
		)
	}
	p.lk.RUnlock()
	if len(nodes) == 0 {
		return ErrNoBackend
	}

	carFetchStart := time.Now()

	pq := []string{path}
	for i := 0; i < len(nodes); i++ {
		if recordIfContextErr(resourceTypeCar, ctx, "fetchResourceWithLoop") {
			return ctx.Err()
		}

		err = p.fetchResourceAndUpdate(ctx, nodes[i], pq[0], i, cb)
		if err != nil && errors.Is(err, context.Canceled) {
			return err
		}

		var epr = ErrPartialResponse{}
		if err == nil {
			pq = pq[1:]
			if len(pq) == 0 {
				durationMs := time.Since(carFetchStart).Milliseconds()
				// TODO: how to account for total retrieved data
				//fetchSpeedPerBlockMetric.Observe(float64(float64(len(blk.RawData())) / float64(durationMs)))
				fetchDurationCarSuccessMetric.Observe(float64(durationMs))
				return
			} else {
				// TODO: potentially worth doing something smarter here based on what the current state
				// of permanent vs temporary errors is.

				// for now: reset i on partials so we also give them a chance to retry.
				i = -1
			}
		} else if errors.As(err, &epr) {
			if len(epr.StillNeed) == 0 {
				// the error was ErrPartial, but no additional needs were specified treat as
				// any other transient error.
				continue
			}
			pq = pq[1:]
			pq = append(pq, epr.StillNeed...)
			// TODO: potentially worth doing something smarter here based on what the current state
			// of permanent vs temporary errors is.

			// for now: reset i on partials so we also give them a chance to retry.
			i = -1
		}
	}

	fetchDurationCarFailureMetric.Observe(float64(time.Since(carFetchStart).Milliseconds()))
	p.updateFetchKeyCoolDown(path)

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

func (p *pool) fetchBlockAndUpdate(ctx context.Context, node string, c cid.Cid, attempt int) (blk blocks.Block, err error) {
	blk, rm, err := p.doFetch(ctx, node, c, attempt)
	if err != nil && errors.Is(err, context.Canceled) {
		return nil, err
	}
	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", c, "error", err)
	}

	err = p.commonUpdate(node, rm, err)
	return
}

func (p *pool) fetchResourceAndUpdate(ctx context.Context, node string, path string, attempt int, cb DataCallback) (err error) {
	rm, err := p.fetchResource(ctx, node, path, "application/vnd.ipld.car", attempt, cb)
	if err != nil && errors.Is(err, context.Canceled) {
		return err
	}
	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", path, "error", err)
	}

	p.commonUpdate(node, rm, err)
	return
}

func (p *pool) commonUpdate(node string, rm tieredhashing.ResponseMetrics, err error) (ferr error) {
	p.lk.Lock()
	defer p.lk.Unlock()

	ferr = err
	if err == nil && rm.Success {
		p.th.RecordSuccess(node, rm)

		if p.th.IsInitDone() {
			p.poolInitDone.Do(func() {
				poolEnoughObservationsForMainSetDurationMetric.Set(float64(time.Since(p.th.StartAt).Milliseconds()))
			})
		}

		// Saturn fetch worked, we return the block.
		return
	}

	fr := p.th.RecordFailure(node, rm)
	if fr != nil {
		poolRemovedFailureTotalMetric.WithLabelValues(string(fr.Tier), fr.Reason).Inc()
		poolRemovedConnFailureTotalMetric.WithLabelValues(string(fr.Tier)).Add(float64(fr.ConnErrors))
		poolRemovedReadFailureTotalMetric.WithLabelValues(string(fr.Tier)).Add(float64(fr.NetworkErrors))
		poolRemovedNon2xxTotalMetric.WithLabelValues(string(fr.Tier)).Add(float64(fr.ResponseCodes))

		if fr.MainToUnknownChange != 0 || fr.UnknownToMainChange != 0 {
			poolTierChangeMetric.WithLabelValues(tierMainToUnknown).Set(float64(fr.MainToUnknownChange))
			poolTierChangeMetric.WithLabelValues(tierUnknownToMain).Set(float64(fr.UnknownToMainChange))
		}
	}

	if p.th.DoRefresh() {
		select {
		case p.refresh <- struct{}{}:
		default:
		}
	}

	return
}
