package caboose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/google/uuid"
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
	coolOffCount     map[string]int     // guarded by lk
	coolOffCache     *cache.Cache       // guarded by lk
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

func NewMemberWithWeight(addr string, weight int, lastUpdateTime time.Time) *Member {
	return &Member{url: addr, lk: sync.Mutex{}, lastUpdate: lastUpdateTime, replication: weight}
}

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
				nm.replication = (m.replication * 80) / 100
				return nm, true
			} else {
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
		coolOffCount:     make(map[string]int),
		coolOffCache:     cache.New(c.TooManyReqsCoolOff, cache.DefaultExpiration),
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
			// add back node with lower weight if it was removed recently.
			if _, ok := p.removedTimeCache.Get(s); ok {
				if _, ok := oldMap[s]; !ok {
					n = append(n, NewMemberWithWeight(s, defaultReplication/2, time.Time{}))
					continue
				}
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

func (p *pool) fetchWith(ctx context.Context, c cid.Cid, with string) (blk blocks.Block, err error) {
	// wait for pool to be initialised
	<-p.started

	transientErrs := make(map[string]error)
	nodes, err := p.getNodesToFetch(c, with)
	if err != nil {
		return nil, err
	}

	blockFetchStart := time.Now()
	for i := 0; i < len(nodes); i++ {
		blk, err = p.fetchAndUpdate(ctx, nodes[i], c, i, transientErrs)

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

func (p *pool) getNodesToFetch(c cid.Cid, with string) ([]string, error) {
	p.lk.RLock()
	defer p.lk.RUnlock()

	refreshFnc := func() {
		select {
		case p.refresh <- struct{}{}:
		default:
		}
	}

	left := p.config.MaxRetrievalAttempts
	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}

	// Get min(maxRetrievalAttempts, len(endpoints)) nodes from the consistent hashing ring for the given cid.
	if left > len(p.endpoints) {
		left = len(p.endpoints)
	}

	if p.c == nil || p.c.Size() == 0 {
		return nil, ErrNoBackend
	}
	nodes, ok := p.c.GetNodes(aff, left)

	// if there are no endpoints in the consistent hashing ring for the given cid, we submit a pool refresh request and fail this fetch.
	if !ok || len(nodes) == 0 {
		refreshFnc()
		return nil, ErrNoBackend
	}

	// filter out cool off nodes
	var withoutCoolOff []string
	withoutCoolOffMap := make(map[string]struct{})
	for _, node := range nodes {
		if _, ok := p.coolOffCache.Get(node); !ok {
			withoutCoolOff = append(withoutCoolOff, node)
			withoutCoolOffMap[node] = struct{}{}
		}
	}
	// if we have enough nodes, we are done.
	if len(withoutCoolOff) >= left {
		return withoutCoolOff, nil
	}

	// trigger a refresh and try to fetch more nodes
	refreshFnc()
	allNodes, ok := p.c.GetNodes(aff, len(p.endpoints))
	if !ok {
		return nil, ErrNoBackend
	}

	for _, node := range allNodes {
		_, wok := withoutCoolOffMap[node]
		_, cok := p.coolOffCache.Get(node)
		if !wok && !cok {
			withoutCoolOff = append(withoutCoolOff, node)
			if len(withoutCoolOff) == left {
				return withoutCoolOff, nil
			}
		}
	}

	// if we still don't have enough nodes, just return the initial set of nodes we got without considering cool off.
	return nodes, nil

}

func (p *pool) fetchAndUpdate(ctx context.Context, node string, c cid.Cid, attempt int, transientErrs map[string]error) (blk blocks.Block, err error) {
	blk, err = p.doFetch(ctx, node, c, attempt)
	if err != nil {
		goLogger.Debugw("fetch attempt failed", "from", node, "attempt", attempt, "of", c, "error", err)
	}

	if err == nil {
		p.changeWeight(node, false, false)
		// Saturn fetch worked, we return the block.
		return
	}

	// If this is a NOT found or Timeout error, park the downvoting for now and see if other members are able to give us this content.
	if errors.Is(err, ErrContentProviderNotFound) || errors.Is(err, ErrSaturnTimeout) {
		transientErrs[node] = err
		return
	}

	coolOff := false
	if errors.Is(err, ErrSaturnTooManyRequests) {
		coolOff = true
	}

	// Saturn fetch failed, we downvote the failing member.
	p.changeWeight(node, true, coolOff)
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
		delete(p.coolOffCount, nm.url)
		p.coolOffCache.Delete(nm.url)

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
func (p *pool) changeWeight(node string, failure bool, coolOff bool) {
	p.lk.Lock()
	defer p.lk.Unlock()

	// reduce weight if we've repeatedly seen a 429 for this node; otherwise, cool off requests to it for a while and try again.
	if coolOff {
		p.coolOffCache.Set(node, struct{}{}, cache.DefaultExpiration)
		val := p.coolOffCount[node]
		p.coolOffCount[node] = val + 1
		if (val + 1) <= MaxNCoolOff {
			return
		}
	}

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

var saturnReqTmpl = "https://%s/ipfs/%s?format=raw"

var (
	saturnNodeIdKey     = "Saturn-Node-Id"
	saturnTransferIdKey = "Saturn-Transfer-Id"
	saturnCacheHitKey   = "Saturn-Cache-Status"
	saturnCacheHit      = "HIT"
)

// doFetch attempts to fetch a block from a given Saturn endpoint. It sends the retrieval logs to the logging endpoint upon a successful or failed attempt.
func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid, attempt int) (b blocks.Block, e error) {
	requestId := uuid.NewString()
	goLogger.Debugw("doing fetch", "from", from, "of", c, "requestId", requestId)
	start := time.Now()
	response_success_end := time.Now()

	fb := time.Unix(0, 0)
	code := 0
	proto := "unknown"
	respReq := &http.Request{}
	received := 0
	reqUrl := ""
	var respHeader http.Header
	saturnNodeId := ""
	saturnTransferId := ""
	isCacheHit := false
	networkError := ""

	defer func() {
		ttfbMs := fb.Sub(start).Milliseconds()
		durationSecs := time.Since(start).Seconds()
		durationMs := time.Since(start).Milliseconds()
		goLogger.Debugw("fetch result", "from", from, "of", c, "status", code, "size", received, "ttfb", int(ttfbMs), "duration", durationSecs, "attempt", attempt, "error", e)
		fetchResponseMetric.WithLabelValues(fmt.Sprintf("%d", code)).Add(1)

		if e == nil && received > 0 {
			fetchTTFBPerBlockPerPeerSuccessMetric.Observe(float64(ttfbMs))
			fetchDurationPerBlockPerPeerSuccessMetric.Observe(float64(response_success_end.Sub(start).Milliseconds()))
			fetchSpeedPerBlockPerPeerMetric.Observe(float64(received) / float64(durationMs))
		} else {
			fetchTTFBPerBlockPerPeerFailureMetric.Observe(float64(ttfbMs))
			fetchDurationPerBlockPerPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
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
	reqUrl = fmt.Sprintf(saturnReqTmpl, from, c)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, reqUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/vnd.ipld.raw")
	if p.config.ExtraHeaders != nil {
		for k, vs := range *p.config.ExtraHeaders {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	resp, err := p.config.SaturnClient.Do(req)
	if err != nil {
		networkError = err.Error()
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	respHeader = resp.Header
	defer resp.Body.Close()

	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrSaturnTooManyRequests)
		}
		if resp.StatusCode == http.StatusGatewayTimeout {
			return nil, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrSaturnTimeout)
		}

		// This should only be 502, but L1s were not translating 404 from Lassie, so we have to support both for now.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadGateway {
			return nil, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrContentProviderNotFound)
		}

		return nil, fmt.Errorf("http error from strn: %d", resp.StatusCode)
	}

	block, ttfb, err := ReadAllWithTTFB(io.LimitReader(resp.Body, maxBlockSize))
	fb = ttfb
	received = len(block)

	if err != nil {
		switch {
		case err == io.EOF && received >= maxBlockSize:
			// we don't expect to see this error any time soon, but if IPFS
			// ecosystem ever starts allowing bigger blocks, this message will save
			// multiple people collective man-months in debugging ;-)
			return nil, fmt.Errorf("strn responded with a block bigger than maxBlockSize=%d", maxBlockSize-1)
		case err == io.EOF:
			// This is fine :-)
			// Zero-length block may be valid (example: bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku)
			// We accept this as non-error and let it go over CID validation later.
		default:
			return nil, fmt.Errorf("unable to read strn response body: %w", err)
		}
	}

	if p.config.DoValidation {
		nc, err := c.Prefix().Sum(block)
		if err != nil {
			return nil, blocks.ErrWrongHash
		}
		if !nc.Equals(c) {
			return nil, blocks.ErrWrongHash
		}
	}
	response_success_end = time.Now()

	return blocks.NewBlockWithCid(block, c)
}

func ReadAllWithTTFB(r io.Reader) ([]byte, time.Time, error) {
	b := make([]byte, 0, 512)
	var ttfb time.Time
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if ttfb.IsZero() {
			ttfb = time.Now()
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, ttfb, err
		}
	}
}
