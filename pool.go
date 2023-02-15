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

var l1_discovery_timeout = 1 * time.Minute

// loadPool refreshes the set of Saturn endpoints to fetch cars from the Orchestrator Endpoint
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

func NewMember(addr string) *Member {
	return &Member{url: addr, lk: sync.Mutex{}, lastUpdate: time.Now(), replication: 20}
}

func (m *Member) String() string {
	return string(m.url)
}

func (m *Member) ReplicationFactor() int {
	return m.replication
}

func (m *Member) Downvote(debounce time.Duration) (*Member, bool) {
	// this is a best-effort. if there's a correlated failure we ignore the others, so do the try on best-effort.
	if m.lk.TryLock() {
		if time.Since(m.lastUpdate) > debounce {
			// make the down-voted member
			nm := NewMember(m.url)
			nm.replication = m.replication / 2
			nm.lastUpdate = time.Now()
			m.lk.Unlock()
			return nm, true
		}
		m.lk.Unlock()
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
	go p.refreshPool()
	return &p
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
				n = append(n, NewMember(s))
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

	for i := 0; i < len(nodes); i++ {
		blk, err = p.doFetch(ctx, nodes[i], c)
		// Saturn fetch was successful, we can return the block.
		if err == nil {
			return
		}

		// Saturn fetch failed, we downvote the failing member and try the next one.
		p.lk.RLock()
		idx, nm := p.downVoteMemberUnlocked(nodes[i])
		p.lk.RUnlock()

		// we weren't able to downvote the failing Saturn node, let's just retry the fetch with a new node.
		if idx == -1 || nm == nil {
			continue
		}

		// we need to take the lock as we're updating the list of pool endpoint members below.
		p.lk.Lock()
		if p.endpoints[idx].url == nm.url {
			// if the failing member has been downvoted to 0, we remove it from the pool.
			// if after removing this member from the pool, the size of the pool falls below the low watermark,
			// we attempt a pool refresh.
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
				// if the failing member has been downvotes but not to 0, simply update the new weight
				// in the pool.
				p.endpoints[idx] = nm
				p.c.UpdateWithWeights(p.endpoints.ToWeights())
			}
		}
		p.lk.Unlock()
	}

	// Saturn fetch failed after exhausting all retrieval attempts, we can return the error.
	return
}

func (p *pool) downVoteMemberUnlocked(node string) (index int, member *Member) {
	idx := -1
	var nm *Member
	var needUpdate bool
	for j, m := range p.endpoints {
		if m.String() == node {
			if nm, needUpdate = m.Downvote(p.config.PoolFailureDownvoteDebounce); needUpdate {
				idx = j
			}
			break
		}
	}

	return idx, nm
}

var saturnReqTmpl = "http://%s/ipfs/%s?format=raw"

// doFetch attempts to fetch a block from a given Saturn endpoint. It sends the retrieval logs to the logging endpoint upon a successful or failed attempt.
func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid) (b blocks.Block, e error) {
	requestId := uuid.NewString()
	goLogger.Debugw("doing fetch", "from", from, "of", c, "requestId", requestId)
	start := time.Now()
	fb := time.Now()
	code := 0
	proto := "unknown"
	respReq := &http.Request{}
	received := 0
	defer func() {
		goLogger.Debugw("fetch result", "from", from, "of", c, "status", code, "size", received, "ttfb", int(fb.Sub(start).Milliseconds()), "duration", time.Since(start).Seconds())
		fetchResponseMetric.WithLabelValues(fmt.Sprintf("%d", code)).Add(1)
		if e == nil {
			fetchLatencyMetric.Observe(float64(fb.Sub(start).Milliseconds()))
			fetchSpeedMetric.Observe(float64(received) / time.Since(start).Seconds())
			fetchSizeMetric.Observe(float64(received))
		}
		p.logger.queue <- log{
			CacheHit:  false,
			URL:       from,
			LocalTime: start,
			// TODO: does this include header sizes?
			NumBytesSent:    received,
			RequestDuration: time.Since(start).Seconds(),
			RequestID:       requestId,
			HTTPStatusCode:  code,
			HTTPProtocol:    proto,
			TTFBMS:          int(fb.Sub(start).Milliseconds()),
			// my address
			ClientAddress: "",
			Range:         "",
			Referrer:      respReq.Referer(),
			UserAgent:     respReq.UserAgent(),
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(saturnReqTmpl, from, c), nil)
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
		return nil, err
	}
	defer resp.Body.Close()

	fb = time.Now()
	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request

	// TODO: What if the Saturn node is malicious ? We should have an upper bound on how many bytes we read here.
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	received = len(rb)

	if p.config.DoValidation {
		nc, err := c.Prefix().Sum(rb)
		if err != nil {
			return nil, blocks.ErrWrongHash
		}
		if !nc.Equals(c) {
			return nil, blocks.ErrWrongHash
		}
	} else if resp.StatusCode != http.StatusOK {
		return nil, ErrNoBackend
	}
	return blocks.NewBlockWithCid(rb, c)
}
