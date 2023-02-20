package caboose

import (
	"context"
	"encoding/json"
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

type updateRingMembersReq struct {
	newMembers []string
	done       chan struct{}
}

type getRingNodesForKeyReq struct {
	key  string
	n    int
	resp chan getRingNodesResp
}

type getRingNodesResp struct {
	nodes []string
	ok    bool
	err   error
}

type pool struct {
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	config *Config
	logger *logger

	started chan struct{} // started signals that we've already initialized the pool once with Saturn endpoints.
	refresh chan struct{} // refresh is used to signal the need for doing a refresh of the Saturn endpoints pool.

	// ringLoop vars
	endpoints MemberList         // guarded by ringLoop
	c         *hashring.HashRing // guarded by ringLoop

	updateRingMembersCh  chan updateRingMembersReq
	getRingNodesForKeyCh chan getRingNodesForKeyReq
	upvoteNodeCh         chan string
	downvoteNodeCh       chan string
}

// MemberList is the list of Saturn endpoints that are currently nodes of the Caboose consistent hashing ring
// that determines which Saturn endpoint to use to retrieve a given CID.
type MemberList []*Member

// ToWeights returns a map of Saturn endpoints to their weight on Caboose's consistent hashing ring.
// can ONLY be called from the ring event loop
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

func NewMember(addr string) *Member {
	return &Member{url: addr, lk: sync.Mutex{}, lastUpdate: time.Now(), replication: defaultReplication}
}

func (m *Member) ReplicationFactor() int {
	return m.replication
}

func newPool(c *Config) *pool {
	ctx, cancel := context.WithCancel(context.Background())

	p := pool{
		ctx:     ctx,
		cancel:  cancel,
		config:  c,
		started: make(chan struct{}),
		refresh: make(chan struct{}, 1),

		endpoints:            []*Member{},
		c:                    nil,
		updateRingMembersCh:  make(chan updateRingMembersReq),
		getRingNodesForKeyCh: make(chan getRingNodesForKeyReq),
		upvoteNodeCh:         make(chan string, 256),
		downvoteNodeCh:       make(chan string, 256),
	}

	p.wg.Add(1)
	go p.refreshPool()

	p.wg.Add(1)
	go p.ringLoop()
	return &p
}

func (p *pool) doRefresh() {
	newEP, err := p.loadPool()
	if err != nil {
		goLogger.Errorw("failed to load pool from orchestrator", "err", err)
		poolErrorMetric.Add(1)
		return
	}

	// send request to event loop to update the pool endpoints and wait for update to finish before returning
	doneCh := make(chan struct{}, 1)
	select {
	case p.updateRingMembersCh <- updateRingMembersReq{newMembers: newEP, done: doneCh}:
		select {
		case <-doneCh:
		case <-p.ctx.Done():
			return
		}
	case <-p.ctx.Done():
		return
	}
}

func (p *pool) refreshPool() {
	defer p.wg.Done()
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
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *pool) Close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.wg.Wait()
	})
}

func (p *pool) ringLoop() {
	defer p.wg.Done()
	for {
		select {
		case updateReq := <-p.updateRingMembersCh:
			newMembers := updateReq.newMembers
			// update the pool endpoints
			oldMap := make(map[string]bool)
			n := make([]*Member, 0, len(newMembers))
			for _, o := range p.endpoints {
				oldMap[o.url] = true
				n = append(n, o)
			}

			for _, s := range newMembers {
				if _, ok := oldMap[s]; !ok {
					n = append(n, NewMember(s))
				}
			}

			p.endpoints = n
			if p.c == nil {
				p.c = hashring.NewWithWeights(p.endpoints.ToWeights())
			} else {
				p.c.UpdateWithWeights(p.endpoints.ToWeights())
			}
			poolSizeMetric.Set(float64(len(n)))

			updateReq.done <- struct{}{}

		case getRingMembersReq := <-p.getRingNodesForKeyCh:
			if p.c == nil || p.c.Size() == 0 {
				getRingMembersReq.resp <- getRingNodesResp{nodes: nil, ok: false, err: ErrNoBackend}
				continue
			}

			key := getRingMembersReq.key
			n := getRingMembersReq.n
			if n > len(p.endpoints) {
				n = len(p.endpoints)
			}
			nodes, ok := p.c.GetNodes(key, n)
			getRingMembersReq.resp <- getRingNodesResp{nodes: nodes, ok: ok}

		case node := <-p.upvoteNodeCh:
			for _, m := range p.endpoints {
				if m.url == node {
					if time.Since(m.lastUpdate) > p.config.PoolWeightChangeDebounce {
						m.lastUpdate = time.Now()

						if m.replication < defaultReplication {
							updated := m.replication + 1
							if updated > defaultReplication {
								updated = defaultReplication
							}
							if updated != m.replication {
								m.replication = updated
							}
						}

						p.c.UpdateWithWeights(p.endpoints.ToWeights())
					}
					break
				}
			}

		case node := <-p.downvoteNodeCh:
			for i, m := range p.endpoints {
				if m.url == node {
					if time.Since(m.lastUpdate) > p.config.PoolWeightChangeDebounce {
						m.lastUpdate = time.Now()
						m.replication = m.replication / 2

						if m.replication == 0 {
							p.c = p.c.RemoveNode(m.url)
							p.endpoints = append(p.endpoints[:i], p.endpoints[i+1:]...)
							// trigger a refresh if we are below the low watermark. This is non-blocking, so okay to do it here.
							if len(p.endpoints) < p.config.PoolLowWatermark {
								select {
								case p.refresh <- struct{}{}:
								default:
								}
							}
						} else {
							p.c.UpdateWithWeights(p.endpoints.ToWeights())
						}
					}
					break
				}
			}

		case <-p.ctx.Done():
			return
		}
	}
}

func (p *pool) getNodesForKey(key string, n int) ([]string, error) {
	getRingMembersResp := make(chan getRingNodesResp, 1)
	getRingMembersReq := getRingNodesForKeyReq{
		key:  key,
		n:    n,
		resp: getRingMembersResp,
	}

	select {
	case p.getRingNodesForKeyCh <- getRingMembersReq:
		select {
		case resp := <-getRingMembersResp:
			members, ok := resp.nodes, resp.ok

			// if there are no endpoints in the consistent hashing ring for the given cid, we submit a pool refresh request and fail this fetch.
			if !ok || len(members) == 0 {
				select {
				case p.refresh <- struct{}{}:
				default:
				}
				return nil, ErrNoBackend
			}

			// return the nodes we found
			return members, nil

		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

func (p *pool) fetchWith(ctx context.Context, c cid.Cid, with string) (blocks.Block, error) {
	// wait for pool to be initialised
	<-p.started

	left := p.config.MaxRetrievalAttempts
	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}
	nodes, err := p.getNodesForKey(aff, left)
	if err != nil {
		return nil, err
	}
	var lastErr error

	for i := 0; i < len(nodes); i++ {
		blk, err := p.doFetch(ctx, nodes[i], c, i)
		lastErr = err

		// if there was no error fetching the block, we upvote the node and return the block.
		if err == nil {
			select {
			case p.upvoteNodeCh <- nodes[i]:
			case <-p.ctx.Done():
				return nil, p.ctx.Err()
			}

			return blk, nil
		}

		// otherwise we downvote the node and re-attempt the fetch with the next one.
		select {
		case p.downvoteNodeCh <- nodes[i]:
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	}

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return nil, lastErr
}

var saturnReqTmpl = "https://%s/ipfs/%s?format=raw"

// doFetch attempts to fetch a block from a given Saturn endpoint. It sends the retrieval logs to the logging endpoint upon a successful or failed attempt.
func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid, attempt int) (b blocks.Block, e error) {
	requestId := uuid.NewString()
	goLogger.Debugw("doing fetch", "from", from, "of", c, "requestId", requestId)
	start := time.Now()
	fb := time.Unix(0, 0)
	code := 0
	proto := "unknown"
	respReq := &http.Request{}
	received := 0
	defer func() {
		ttfbMs := fb.Sub(start).Milliseconds()
		durationSecs := time.Since(start).Seconds()
		goLogger.Debugw("fetch result", "from", from, "of", c, "status", code, "size", received, "ttfb", int(ttfbMs), "duration", durationSecs, "attempt", attempt, "error", e)
		fetchResponseMetric.WithLabelValues(fmt.Sprintf("%d", code)).Add(1)
		if fb.After(start) {
			fetchLatencyMetric.Observe(float64(ttfbMs))
		}
		if received > 0 {
			fetchSpeedMetric.Observe(float64(received) / durationSecs)
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

	return blocks.NewBlockWithCid(block, c)
}
