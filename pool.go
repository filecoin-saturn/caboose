package caboose

import (
	"context"
	cryptoRand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/asecurityteam/rolling"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/filecoin-saturn/caboose/tieredhashing"

	"github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

const (
	tierMainToUnknown          = "main-to-unknown"
	tierUnknownToMain          = "unknown-to-main"
	BackendOverrideKey         = "CABOOSE_BACKEND_OVERRIDE"
	defaultMirroredConcurrency = 5
)

var complianceCidReqTemplate = "/ipfs/%s?format=car&dag-scope=entity"

// loadPool refreshes the set of Saturn endpoints in the pool by fetching an updated list of responsive Saturn nodes from the
// Saturn Orchestrator.
func (p *pool) loadPool() ([]tieredhashing.NodeInfo, error) {

	if p.config.OrchestratorOverride != nil {
		return p.config.OrchestratorOverride, nil
	}
	client := p.config.OrchestratorClient

	req, err := http.NewRequest("GET", p.config.OrchestratorEndpoint.String(), nil)

	if err != nil {
		goLogger.Warnw("failed to create request to orchestrator", "err", err, "endpoint", p.config.OrchestratorEndpoint)
		return nil, err
	}
	resp, err := client.Do(req)

	if err != nil {
		goLogger.Warnw("failed to get backends from orchestrator", "err", err, "endpoint", p.config.OrchestratorEndpoint)
		return nil, err
	}
	defer resp.Body.Close()

	responses := make([]tieredhashing.NodeInfo, 0)

	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		goLogger.Warnw("failed to decode backends from orchestrator", "err", err, "endpoint", p.config.OrchestratorEndpoint.String())
		return nil, err
	}

	goLogger.Debugw("got backends from orchestrator", "backends", responses, "endpoint", p.config.OrchestratorEndpoint.String())
	goLogger.Infow("got backends from orchestrator", "cnt", len(responses), "endpoint", p.config.OrchestratorEndpoint.String())
	return responses, nil
}

type mirroredPoolRequest struct {
	node string
	path string
	// the key for node affinity for the request
	key string
}

type pool struct {
	config *Config
	logger *logger

	started       chan struct{} // started signals that we've already initialized the pool once with Saturn endpoints.
	refresh       chan struct{} // refresh is used to signal the need for doing a refresh of the Saturn endpoints pool.
	done          chan struct{} // done is used to signal that we're shutting down the Saturn endpoints pool and don't need to refresh it anymore.
	mirrorSamples chan mirroredPoolRequest

	fetchKeyLk            sync.RWMutex
	fetchKeyFailureCache  *cache.Cache // guarded by fetchKeyLk
	fetchKeyCoolDownCache *cache.Cache // guarded by fetchKeyLk

	lk sync.RWMutex
	th *tieredhashing.TieredHashing

	poolInitDone sync.Once
}

func newPool(c *Config) *pool {
	noRemove := false
	if len(os.Getenv(BackendOverrideKey)) > 0 {
		noRemove = true
	}

	topts := append(c.TieredHashingOpts, tieredhashing.WithNoRemove(noRemove))

	p := pool{
		config:        c,
		started:       make(chan struct{}),
		refresh:       make(chan struct{}, 1),
		done:          make(chan struct{}, 1),
		mirrorSamples: make(chan mirroredPoolRequest, 10),

		fetchKeyCoolDownCache: cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),
		fetchKeyFailureCache:  cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),
		th:                    tieredhashing.New(topts...),
	}

	return &p
}

func (p *pool) Start() {
	go p.refreshPool()
	go p.checkPool()
}

func (p *pool) doRefresh() {
	newEP, err := p.loadPool()
	if err == nil {
		p.refreshWithNodes(newEP)
	} else {
		poolRefreshErrorMetric.Add(1)
	}
}

func (p *pool) refreshWithNodes(newEP []tieredhashing.NodeInfo) {
	p.lk.Lock()
	defer p.lk.Unlock()

	// for tests to pass the -race check when accessing global vars
	distLk.Lock()
	defer distLk.Unlock()

	added, alreadyRemoved, back := p.th.AddOrchestratorNodes(newEP)
	poolNewMembersMetric.Set(float64(added))
	poolMembersNotAddedBecauseRemovedMetric.Set(float64(alreadyRemoved))
	poolMembersRemovedAndAddedBackMetric.Set(float64(back))

	p.th.UpdateAverageCorrectnessPct()

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

func (p *pool) fetchComplianceCid(node string) error {
	sc, err := p.th.GetComplianceCid(node)
	if err != nil {
		goLogger.Warnw("failed to find compliance cid ", "err", err)
		return err
	}
	trialTimeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	reqUrl := fmt.Sprintf(complianceCidReqTemplate, sc)
	goLogger.Debugw("fetching compliance cid", "cid", reqUrl, "from", node)
	err = p.fetchResourceAndUpdate(trialTimeout, node, reqUrl, 0, p.mirrorValidator)
	cancel()
	return err
}

func (p *pool) checkPool() {
	sem := make(chan struct{}, defaultMirroredConcurrency)

	for {
		select {
		case msg := <-p.mirrorSamples:
			sem <- struct{}{}
			go func(msg mirroredPoolRequest) {
				defer func() { <-sem }()

				p.lk.RLock()
				testNodes := p.th.GetNodes(tieredhashing.TierUnknown, msg.key, 1)
				p.lk.RUnlock()
				if len(testNodes) == 0 {
					return
				}
				node := testNodes[0]

				// Send compliance cid
				rand := big.NewInt(1)
				if p.config.ComplianceCidPeriod > 0 {
					rand, _ = cryptoRand.Int(cryptoRand.Reader, big.NewInt(p.config.ComplianceCidPeriod))
				}

				if rand.Cmp(big.NewInt(0)) == 0 {
					_ = p.fetchComplianceCid(node)
					complianceCidCallsTotalMetric.WithLabelValues("success").Add(1)
				}

				// see if it is to a main-tier node - if so find appropriate test node to test against.
				// --- Mirroring
				p.lk.RLock()
				if p.th.NodeTier(msg.node) != tieredhashing.TierMain {
					p.lk.RUnlock()
					return
				}
				p.lk.RUnlock()
				trialTimeout, cancel := context.WithTimeout(context.Background(), DefaultSaturnMirrorRequestTimeout)
				err := p.fetchResourceAndUpdate(trialTimeout, node, msg.path, 0, p.mirrorValidator)

				cancel()
				if err != nil {
					goLogger.Warnw("mirrored request failed", "err", err.Error())

					if strings.Contains(err.Error(), "empty car") {
						mirroredTrafficTotalMetric.WithLabelValues("error-empty-car").Inc()
					}

					mirroredTrafficTotalMetric.WithLabelValues("error").Inc()
				} else {
					mirroredTrafficTotalMetric.WithLabelValues("no-error").Inc()
				}
			}(msg)

		case <-p.done:
			return
		}
	}
}

// TODO: this should be replaced with a real validator once one exists from boxo.
func (p *pool) mirrorValidator(resource string, reader io.Reader) error {
	// first get the 'path' part to remove query string if present.
	pth, err := url.Parse(resource)
	if err != nil {
		return err
	}
	parse, err := path.ParsePath(pth.Path)
	if err != nil {
		return err
	}
	matchedCid := cid.Undef
	if parse.IsJustAKey() && len(parse) == 1 {
		matchedCid, err = cid.Parse(parse.Segments()[0])
	} else if len(parse) > 1 {
		matchedCid, err = cid.Parse(parse.Segments()[1])
	} else {
		err = fmt.Errorf("unrecognized resource: %s", resource)
	}
	if err != nil {
		return err
	}

	br, err := car.NewCarReaderWithOptions(reader, car.WithErrorOnEmptyRoots(false))
	if err != nil {
		return err
	}
	has := false
	for {
		blk, err := br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if matchedCid.Equals(blk.Cid()) {
			has = true
		}
	}
	if !has {
		return fmt.Errorf("response did not have requested root")
	}
	return nil
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
	if len(nodes) == 0 {
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

			// mirror successful request
			if p.config.MirrorFraction > rand.Float64() {
				select {
				case p.mirrorSamples <- mirroredPoolRequest{node: nodes[i], path: fmt.Sprintf("/ipfs/%s?format=car&car-scope=block", c), key: aff}:
				default:
				}
			}

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
	} else {
		goLogger.Infow("using all main set nodes for CAR", "path", path, "aff", aff, "numNodes", len(nodes))
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

		// sample request for mirroring
		if p.config.MirrorFraction > rand.Float64() {
			select {
			case p.mirrorSamples <- mirroredPoolRequest{node: nodes[i], path: pq[0], key: aff}:
			default:
			}
		}
		old := pq[0]
		err = p.fetchResourceAndUpdate(ctx, nodes[i], old, i, cb)
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
			} else if pq[0] == old {
				continue
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

			if pq[0] == old {
				continue
			} else {
				// TODO: potentially worth doing something smarter here based on what the current state
				// of permanent vs temporary errors is.

				// for now: reset i on partials so we also give them a chance to retry.
				i = -1
			}
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
