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

// loadPool refreshes the set of endpoints to fetch cars from from the Orchestrator Endpoint
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
	config    *Config
	endpoints MemberList
	logger    *logger
	c         *hashring.HashRing
	lk        sync.RWMutex
	started   chan struct{}
	refresh   chan struct{}
	done      chan struct{}
}

type MemberList []*Member

func (m MemberList) ToWeights() map[string]int {
	ml := make(map[string]int, len(m))
	for _, mm := range m {
		ml[mm.string] = mm.replication
	}
	return ml
}

type Member struct {
	string
	sync.Mutex
	lastUpdate  time.Time
	replication int
}

func NewMember(addr string) *Member {
	return &Member{addr, sync.Mutex{}, time.Now(), 20}
}

func (m *Member) String() string {
	return string(m.string)
}

func (m *Member) ReplicationFactor() int {
	return m.replication
}

func (m *Member) Downvote(debounce time.Duration) (*Member, bool) {
	// this is a best-effort. if there's a correlated failure we ignore the others, so do the try on best-effort.
	if m.TryLock() {
		if time.Since(m.lastUpdate) > debounce {
			// make the down-voted member
			nm := NewMember(m.string)
			nm.replication = m.replication / 2
			m.lastUpdate = time.Now()
			m.Unlock()
			return nm, true
		}
		m.Unlock()
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
	<-p.started

	left := p.config.MaxRetries
	if left > len(p.endpoints) {
		left = len(p.endpoints)
	}

	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}

	p.lk.RLock()
	if p.c == nil || p.c.Size() == 0 {
		p.lk.RUnlock()
		return nil, ErrNoBackend
	}
	nodes, ok := p.c.GetNodes(aff, left)
	p.lk.RUnlock()
	if !ok || len(nodes) == 0 {
		select {
		case p.refresh <- struct{}{}:
		default:
		}

		return nil, ErrNoBackend
	}

	for i := 0; i < len(nodes); i++ {
		blk, err = p.doFetch(ctx, nodes[i], c)
		if err != nil {
			p.lk.RLock()
			idx := -1
			var nm *Member
			var needUpdate bool
			for j, m := range p.endpoints {
				if m.String() == nodes[i] {
					if nm, needUpdate = m.Downvote(p.config.PoolFailureDownvoteDebounce); needUpdate {
						idx = j
					}
					break
				}
			}
			p.lk.RUnlock()
			if idx != -1 {
				p.lk.Lock()
				if p.endpoints[idx].string == nm.string {
					if nm.replication == 0 {
						p.c = p.c.RemoveNode(nm.string)
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
				p.lk.Unlock()
			}
			continue
		}

		return
	}
	return
}

var tmpl = "http://%s/ipfs/%s?format=raw"

func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid) (b blocks.Block, e error) {
	goLogger.Debugw("doing fetch", "from", from, "of", c)
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
			URL:       "",
			LocalTime: start,
			// TODO: does this include header sizes?
			NumBytesSent:    received,
			RequestDuration: time.Since(start).Seconds(),
			RequestID:       uuid.NewString(),
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(tmpl, from, c), nil)
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

	resp, err := p.config.Client.Do(req)
	if err != nil {
		return nil, err
	}
	fb = time.Now()
	code = resp.StatusCode
	proto = resp.Proto
	respReq = resp.Request
	defer resp.Body.Close()
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
