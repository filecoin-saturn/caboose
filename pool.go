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

	"github.com/patrickmn/go-cache"

	"github.com/ipfs/go-cid"
	blocks "github.com/ipfs/go-libipfs/blocks"
	"github.com/serialx/hashring"
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
	goLogger.Infow("got backends from orchestrator", "cnt", len(responses), "endpoint", p.config.OrchestratorEndpoint.String())
	return responses, nil
}

type pool struct {
	config *Config
	logger *logger

	started chan struct{} // started signals that we've already initialized the pool once with Saturn endpoints.
	refresh chan struct{} // refresh is used to signal the need for doing a refresh of the Saturn endpoints pool.
	done    chan struct{} // done is used to signal that we're shutting down the Saturn endpoints pool and don't need to refresh it anymore.

	lk               sync.RWMutex
	endpoints        MemberList         // guarded by lk
	c                *hashring.HashRing // guarded by lk
	removedTimeCache *cache.Cache       // guarded by lk
}

// MemberList is the list of Saturn endpoints that are currently members of the Caboose consistent hashing ring
// that determines which Saturn endpoint to use to retrieve a given CID.
type MemberList []*Member

// ToWeights returns a map of Saturn endpoints to their weight on Caboose's consistent hashing ring.
func (m MemberList) ToWeights() map[string]int {
	ml := make(map[string]int, len(m))
	for _, mm := range m {
		ml[mm.url] = mm.replication
	}
	return ml
}

// Member is a Saturn endpoint that is currently a member of the Caboose consistent hashing ring.
type Member struct {
	lk sync.Mutex

	url         string
	lastUpdate  time.Time
	replication int
}

var defaultReplication = 20

func NewMember(addr string, lastUpdateTime time.Time) *Member {
	return &Member{url: addr, lk: sync.Mutex{}, lastUpdate: lastUpdateTime, replication: defaultReplication}
}

func (m *Member) String() string {
	return string(m.url)
}

func (m *Member) ReplicationFactor() int {
	return m.replication
}

func (m *Member) UpdateWeight(debounce time.Duration, failure bool) (*Member, bool) {
	// this is a best-effort. if there's a correlated failure we ignore the others, so do the try on best-effort.
	if m.lk.TryLock() {
		defer m.lk.Unlock()
		if debounce == 0 || time.Since(m.lastUpdate) > debounce {
			// make the down-voted member
			nm := NewMember(m.url, time.Now())
			if failure {
				nm.replication = m.replication / 2
				return nm, true
			} else {
				// bump by 20 percent only if the current replication factor is less than 20.
				if m.replication < defaultReplication {
					updated := m.replication + 1
					if updated > defaultReplication {
						updated = defaultReplication
					}
					if updated != m.replication {
						nm.replication = updated
						return nm, true
					}
				}
			}
			return nm, false
		}
	}
	return nil, false
}

func newPool(c *Config) *pool {
	p := pool{
		config:           c,
		endpoints:        []*Member{},
		c:                nil,
		started:          make(chan struct{}),
		refresh:          make(chan struct{}, 1),
		done:             make(chan struct{}, 1),
		removedTimeCache: cache.New(c.PoolMembershipDebounce, 10*time.Second),
	}

	return &p
}

func (p *pool) Start() {
	go p.refreshPool()
}

func (p *pool) doRefresh() {
	newEP, err := p.loadPool()
	if err == nil {
		p.lk.Lock()
		defer p.lk.Unlock()

		// TODO: The orchestrator periodically prunes "bad" L1s based on a reputation system
		// it owns and runs. We should probably just forget about the Saturn endpoints that were
		// previously in the pool but are no longer being returned by the orchestrator. It's highly
		// likely that the Orchestrator has deemed them to be non-functional/malicious.
		// Let's just override the old pool with the new endpoints returned here.
		oldMap := make(map[string]bool)
		n := make([]*Member, 0, len(newEP))
		for _, o := range p.endpoints {
			oldMap[o.String()] = true
			n = append(n, o)
		}

		for _, s := range newEP {
			// do not add a node back to the pool if it was removed recently.
			if _, ok := p.removedTimeCache.Get(s); ok {
				continue
			}

			if _, ok := oldMap[s]; !ok {
				// we set last update time to zero so we do NOT hit debounce limits for this node immediately on creation.
				n = append(n, NewMember(s, time.Time{}))
			}
		}

		p.endpoints = n
		if p.c == nil {
			p.c = hashring.NewWithWeights(p.endpoints.ToWeights())
		} else {
			p.c.UpdateWithWeights(p.endpoints.ToWeights())
		}
		poolSizeMetric.Set(float64(len(n)))

		// periodic update of a pool health metric
		byWeight := make(map[int]int)
		for _, m := range p.endpoints {
			if _, ok := byWeight[m.replication]; !ok {
				byWeight[m.replication] = 0
			}
			byWeight[m.replication] += 1
		}
		poolHealthMetric.Reset()
		for weight, cnt := range byWeight {
			poolHealthMetric.WithLabelValues(fmt.Sprintf("%d", weight)).Set(float64(cnt))
		}

	} else {
		poolErrorMetric.Add(1)
	}
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

func (p *pool) fetchBlockWith(ctx context.Context, c cid.Cid, with string) (blk blocks.Block, err error) {
	// wait for pool to be initialised
	<-p.started

	transientErrs := make(map[string]error)

	left := p.config.MaxRetrievalAttempts
	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}

	p.lk.RLock()
	if left > len(p.endpoints) {
		left = len(p.endpoints)
	}

	// if there are no endpoints in the consistent hashing ring, we submit a pool refresh request and fail this fetch.
	if p.c == nil || p.c.Size() == 0 {
		p.lk.RUnlock()
		return nil, ErrNoBackend
	}
	nodes, ok := p.c.GetNodes(aff, left)
	p.lk.RUnlock()

	// if there are no endpoints in the consistent hashing ring for the given cid, we submit a pool refresh request and fail this fetch.
	if !ok || len(nodes) == 0 {
		select {
		case p.refresh <- struct{}{}:
		default:
		}

		return nil, ErrNoBackend
	}

	blockFetchStart := time.Now()

	for i := 0; i < len(nodes); i++ {
		blk, err = p.fetchBlockAndUpdate(ctx, nodes[i], c, i, transientErrs)

		if err == nil {
			durationMs := time.Since(blockFetchStart).Milliseconds()
			fetchSpeedPerBlockMetric.Observe(float64(float64(len(blk.RawData())) / float64(durationMs)))
			fetchDurationBlockSuccessMetric.Observe(float64(durationMs))

			// downvote all parked failed nodes as some other node was able to give us the required content here.
			reqs := make([]weightUpdateReq, 0, len(transientErrs))
			for node, err := range transientErrs {
				goLogger.Debugw("downvoting node with transient err as fetch was subsequently successful", "node", node, "err", err)
				reqs = append(reqs, weightUpdateReq{
					node:    node,
					failure: true,
				})
			}

			p.updateWeightBatched(reqs)
			return
		}
	}

	fetchDurationBlockFailureMetric.Observe(float64(time.Since(blockFetchStart).Milliseconds()))

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

func (p *pool) fetchResourceWith(ctx context.Context, path string, cb DataCallback, with string) (err error) {
	// wait for pool to be initialised
	<-p.started

	transientErrs := make(map[string]error)

	left := p.config.MaxRetrievalAttempts
	aff := with
	if aff == "" {
		aff = path
	}

	p.lk.RLock()
	if left > len(p.endpoints) {
		left = len(p.endpoints)
	}

	// if there are no endpoints in the consistent hashing ring, we submit a pool refresh request and fail this fetch.
	if p.c == nil || p.c.Size() == 0 {
		p.lk.RUnlock()
		return ErrNoBackend
	}
	nodes, ok := p.c.GetNodes(aff, left)
	p.lk.RUnlock()

	// if there are no endpoints in the consistent hashing ring for the given cid, we submit a pool refresh request and fail this fetch.
	if !ok || len(nodes) == 0 {
		select {
		case p.refresh <- struct{}{}:
		default:
		}

		return ErrNoBackend
	}

	carFetchStart := time.Now()

	pq := []string{path}
	for i := 0; i < len(nodes); i++ {
		err = p.fetchResourceAndUpdate(ctx, nodes[i], pq[0], i, cb, transientErrs)

		if err == nil {
			pq = pq[1:]
			if len(pq) == 0 {
				durationMs := time.Since(carFetchStart).Milliseconds()
				// TODO: how to account for total retrieved data
				//fetchSpeedPerBlockMetric.Observe(float64(float64(len(blk.RawData())) / float64(durationMs)))
				fetchDurationCarSuccessMetric.Observe(float64(durationMs))

				// downvote all parked failed nodes as some other node was able to give us the required content here.
				reqs := make([]weightUpdateReq, 0, len(transientErrs))
				for node, err := range transientErrs {
					goLogger.Debugw("downvoting node with transient err as fetch was subsequently successful", "node", node, "err", err)
					reqs = append(reqs, weightUpdateReq{
						node:    node,
						failure: true,
					})
				}

				p.updateWeightBatched(reqs)
				return
			} else {
				// TODO: potentially worth doing something smarter here based on what the current state
				// of permanent vs temporary errors is.

				// for now: reset i on partials so we also give them a chance to retry.
				i = 0
			}
		} else if errors.Is(err, ErrPartialResponse{}) {
			epe, ok := err.(ErrPartialResponse)
			if !ok {
				continue
			}
			if len(epe.StillNeed) == 0 {
				// the error was ErrPartial, but no additional needs were specified treat as
				// any other transient error.
				continue
			}
			pq = pq[1:]
			pq = append(pq, epe.StillNeed...)
			// TODO: potentially worth doing something smarter here based on what the current state
			// of permanent vs temporary errors is.

			// for now: reset i on partials so we also give them a chance to retry.
			i = 0
		}
	}

	fetchDurationCarFailureMetric.Observe(float64(time.Since(carFetchStart).Milliseconds()))

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

func (p *pool) fetchBlockAndUpdate(ctx context.Context, node string, c cid.Cid, attempt int, transientErrs map[string]error) (blk blocks.Block, err error) {
	blk, err = p.doFetch(ctx, node, c, attempt)
	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", c, "error", err)
	}

	if err == nil {
		p.changeWeight(node, false)
		// Saturn fetch worked, we return the block.
		return
	}

	// If this is a NOT found or Timeout error, park the downvoting for now and see if other members are able to give us this content.
	if errors.Is(err, ErrContentProviderNotFound) || errors.Is(err, ErrSaturnTimeout) {
		transientErrs[node] = err
		return
	}

	// Saturn fetch failed, we downvote the failing member.
	p.changeWeight(node, true)
	return
}

func (p *pool) fetchResourceAndUpdate(ctx context.Context, node string, path string, attempt int, cb DataCallback, transientErrs map[string]error) (err error) {
	err = p.fetchResource(ctx, node, path, "application/vnd.ipld.car", attempt, cb)
	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", path, "error", err)
	}

	if err == nil {
		p.changeWeight(node, false)
		// Saturn fetch worked, we return the block.
		return
	}

	// If this is a NOT found or Timeout error, park the downvoting for now and see if other members are able to give us this content.
	if errors.Is(err, ErrContentProviderNotFound) || errors.Is(err, ErrSaturnTimeout) || errors.Is(err, ErrPartialResponse{}) {
		transientErrs[node] = err
		return
	}

	// Saturn fetch failed, we downvote the failing member.
	p.changeWeight(node, true)
	return
}

type weightUpdateReq struct {
	node    string
	failure bool
}

func (p *pool) updateWeightBatched(reqs []weightUpdateReq) {
	p.lk.Lock()
	defer p.lk.Unlock()

	for _, req := range reqs {
		idx, nm := p.updateWeightUnlocked(req.node, req.failure)
		// we weren't able to change the weight.
		if idx == -1 || nm == nil {
			continue
		}
		p.updatePoolWithNewWeightUnlocked(nm, idx)
	}
}

func (p *pool) updatePoolWithNewWeightUnlocked(nm *Member, idx int) {
	if nm.replication == 0 {
		p.c = p.c.RemoveNode(nm.url)
		p.endpoints = append(p.endpoints[:idx], p.endpoints[idx+1:]...)
		// we will not add this node back to the cache before the cool off period expires
		p.removedTimeCache.Set(nm.url, struct{}{}, cache.DefaultExpiration)
		if len(p.endpoints) < p.config.PoolLowWatermark {
			select {
			case p.refresh <- struct{}{}:
			default:
			}
		}
	} else {
		p.endpoints[idx] = nm
		p.c.UpdateWithWeights(p.endpoints.ToWeights())
	}
}

// returns the updated weight mapping for tests
func (p *pool) changeWeight(node string, failure bool) {
	p.lk.Lock()
	defer p.lk.Unlock()

	idx, nm := p.updateWeightUnlocked(node, failure)

	// we weren't able to change the weight.
	if idx == -1 || nm == nil {
		return
	}

	p.updatePoolWithNewWeightUnlocked(nm, idx)
}

func (p *pool) updateWeightUnlocked(node string, failure bool) (index int, member *Member) {
	idx := -1
	var nm *Member
	var needUpdate bool
	for j, m := range p.endpoints {
		if m.String() == node {
			if nm, needUpdate = m.UpdateWeight(p.config.PoolWeightChangeDebounce, failure); needUpdate {
				idx = j
			}
			break
		}
	}
	return idx, nm
}
