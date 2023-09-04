package caboose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/ipfs/boxo/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

const blockPathPattern = "/ipfs/%s?format=car&dag-scope=block"

// loadPool refreshes the set of endpoints in the pool by fetching an updated list of nodes from the
// Orchestrator.
func (p *pool) loadPool() ([]string, error) {
	if p.config.OrchestratorOverride != nil {
		return p.config.OrchestratorOverride, nil
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

type mirroredPoolRequest struct {
	node *Node
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

	ActiveNodes *NodeRing
	AllNodes    *NodeHeap
}

func newPool(c *Config, logger *logger) *pool {
	p := pool{
		config:        c,
		logger:        logger,
		started:       make(chan struct{}),
		refresh:       make(chan struct{}, 1),
		done:          make(chan struct{}, 1),
		mirrorSamples: make(chan mirroredPoolRequest, 10),

		fetchKeyCoolDownCache: cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),
		fetchKeyFailureCache:  cache.New(c.FetchKeyCoolDownDuration, 1*time.Minute),

		ActiveNodes: NewNodeRing(),
		AllNodes:    NewNodeHeap(),
	}

	return &p
}

func (p *pool) Start() {
	go p.refreshPool()
	go p.checkPool()
}

func (p *pool) DoRefresh() {
	newEP, err := p.loadPool()
	if err == nil {
		for _, n := range newEP {
			node := NewNode(n)
			p.AllNodes.AddIfNotPresent(node)
		}
	} else {
		poolRefreshErrorMetric.Add(1)
	}
	if err := updateActiveNodes(p.ActiveNodes, p.AllNodes); err != nil {
		goLogger.Warnw("failed to update active nodes", "error", err)
	}
}

// refreshPool is a background thread triggering `DoRefresh` every `config.PoolRefresh` interval.
func (p *pool) refreshPool() {
	t := time.NewTimer(0)
	started := sync.Once{}
	for {
		select {
		case <-t.C:
			p.DoRefresh()
			started.Do(func() {
				close(p.started)
			})

			t.Reset(p.config.PoolRefresh)
		case <-p.refresh:
			p.DoRefresh()
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

func (p *pool) checkPool() {
	for {
		select {
		case msg := <-p.mirrorSamples:
			// see if it is to a main-tier node - if so find appropriate test node to test against.
			if !p.ActiveNodes.Contains(msg.node) {
				continue
			}
			testNode := p.AllNodes.PeekRandom()
			if testNode == nil {
				continue
			}
			if p.ActiveNodes.Contains(testNode) {
				continue
			}

			trialTimeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := p.fetchResourceAndUpdate(trialTimeout, testNode, msg.path, 0, p.mirrorValidator)
			cancel()
			if err != nil {
				mirroredTrafficTotalMetric.WithLabelValues("error").Inc()
			} else {
				mirroredTrafficTotalMetric.WithLabelValues("no-error").Inc()
			}
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

	br, err := car.NewCarReader(reader)
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

func (p *pool) fetchBlockWith(ctx context.Context, c cid.Cid, with string) (blk blocks.Block, err error) {
	fetchCalledTotalMetric.WithLabelValues(resourceTypeBlock).Add(1)
	// wait for pool to be initialised
	<-p.started

	cb := func(resource string, reader io.Reader) error {
		br, err := car.NewCarReader(reader)
		if err != nil {
			return err
		}
		b, err := br.Next()
		if err != nil {
			return err
		}
		if b.Cid().Equals(c) {
			blk = b
			return nil
		}
		return blocks.ErrWrongHash
	}
	err = p.fetchResourceWith(ctx, fmt.Sprintf(blockPathPattern, c), cb, with)
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
	if isCtxError(ctx) {
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

	nodes, err := p.ActiveNodes.GetNodes(aff, p.config.MaxRetrievalAttempts)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return ErrNoBackend
	}

	carFetchStart := time.Now()

	pq := []string{path}
	for i := 0; i < len(nodes); i++ {
		if isCtxError(ctx) {
			return ctx.Err()
		}

		// sample request for mirroring
		if p.config.MirrorFraction > rand.Float64() {
			select {
			case p.mirrorSamples <- mirroredPoolRequest{node: nodes[i], path: pq[0], key: aff}:
			default:
			}
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

func (p *pool) fetchResourceAndUpdate(ctx context.Context, node *Node, path string, attempt int, cb DataCallback) (err error) {
	err = p.fetchResource(ctx, node, path, "application/vnd.ipld.car", attempt, cb)

	if err != nil && errors.Is(err, context.Canceled) {
		return err
	}

	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", path, "error", err)
	}

	return
}
