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

	lk        sync.RWMutex
	endpoints MemberList         // guarded by lk
	c         *hashring.HashRing // guarded by lk
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
		config:    c,
		endpoints: []*Member{},
		c:         nil,
		started:   make(chan struct{}),
		refresh:   make(chan struct{}, 1),
		done:      make(chan struct{}, 1),
	}

	return &p
}

func (p *pool) Start() {
	go p.refreshPool()
}

func (p *pool) doRefresh() {
	newEP, err := p.loadPool()
	if err == nil {
		p.lk.RLock()

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

		p.lk.RUnlock()

		for _, s := range newEP {
			if _, ok := oldMap[s]; !ok {
				// we set last update time to zero so we do NOT hit debounce limits for this node immediately on creation.
				n = append(n, NewMember(s, time.Time{}))
			}
		}

		p.lk.Lock()
		p.endpoints = n
		if p.c == nil {
			p.c = hashring.NewWithWeights(p.endpoints.ToWeights())
		} else {
			p.c.UpdateWithWeights(p.endpoints.ToWeights())
		}
		p.lk.Unlock()
		poolSizeMetric.Set(float64(len(n)))
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
		blk, err = p.fetchAndUpdate(ctx, nodes[i], c, i, transientErrs)

		if err == nil {
			durationMs := time.Since(blockFetchStart).Milliseconds()
			fetchSpeedPerBlockMetric.Observe(float64(float64(len(blk.RawData())) / float64(durationMs)))

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

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

func (p *pool) replaceNodeToHaveWeight(nm *Member) {
	// we need to take the lock as we're updating the list of pool endpoint members below.
	p.lk.Lock()
	defer p.lk.Unlock()

	// figure out index
	idx := -1
	for j, m := range p.endpoints {
		if m.String() == nm.String() {
			idx = j
		}
	}
	if idx == -1 {
		// no longer present.
		return
	}

	p.updatePoolWithNewWeightUnlocked(nm, idx)
}

func (p *pool) fetchAndUpdate(ctx context.Context, node string, c cid.Cid, attempt int, transientErrs map[string]error) (blk blocks.Block, err error) {
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

func (p *pool) changeWeight(node string, failure bool) {
	p.lk.RLock()
	idx, nm := p.updateWeightUnlocked(node, failure)
	p.lk.RUnlock()

	// we weren't able to change the weight.
	if idx == -1 || nm == nil {
		return
	}

	p.replaceNodeToHaveWeight(nm)
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
	defer func() {
		ttfbMs := fb.Sub(start).Milliseconds()
		durationSecs := time.Since(start).Seconds()
		durationMs := time.Since(start).Milliseconds()
		goLogger.Debugw("fetch result", "from", from, "of", c, "status", code, "size", received, "ttfb", int(ttfbMs), "duration", durationSecs, "attempt", attempt, "error", e)
		fetchResponseMetric.WithLabelValues(fmt.Sprintf("%d", code)).Add(1)

		if e == nil && received > 0 {
			fetchDurationPeerSuccessMetric.Observe(float64(response_success_end.Sub(start).Milliseconds()))
		} else {
			fetchDurationPeerFailureMetric.Observe(float64(time.Since(start).Milliseconds()))
		}

		if received > 0 {
			fetchSpeedPerPeerMetric.Observe(float64(received) / float64(durationMs))
			fetchSizeMetric.Observe(float64(received))
		}
		p.logger.queue <- log{
			CacheHit:  false,
			URL:       from,
			LocalTime: start,
			// TODO: does this include header sizes?
			NumBytesSent:    received,
			RequestDuration: durationSecs,
			RequestID:       requestId,
			HTTPStatusCode:  code,
			HTTPProtocol:    proto,
			TTFBMS:          int(ttfbMs),
			// my address
			ClientAddress: "",
			Range:         "",
			Referrer:      respReq.Referer(),
			UserAgent:     respReq.UserAgent(),
		}
	}()

	reqCtx, cancel := context.WithTimeout(ctx, DefaultSaturnRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fmt.Sprintf(saturnReqTmpl, from, c), nil)
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
	fb = time.Now()
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusGatewayTimeout {
			return nil, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrSaturnTimeout)
		}

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("http error from strn: %d, err=%w", resp.StatusCode, ErrContentProviderNotFound)
		}

		return nil, fmt.Errorf("http error from strn: %d", resp.StatusCode)
	}

	block, err := io.ReadAll(io.LimitReader(resp.Body, maxBlockSize))
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
