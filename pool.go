package caboose

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	blocks "github.com/ipfs/go-libipfs/blocks"
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
	endpoints []Member
	logger    *logger
	c         *consistent.Consistent
	lk        sync.RWMutex
	started   chan struct{}
	done      chan struct{}
}

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// todo: can just prefix good multihashes, probably
	sha := sha256.Sum256(data)
	return binary.BigEndian.Uint64(sha[0:8])
}

func newPool(c *Config) *pool {
	p := pool{
		config:    c,
		endpoints: []Member{},
		c:         nil,
		started:   make(chan struct{}),
		done:      make(chan struct{}, 1),
	}
	go p.refreshPool()
	return &p
}

func (p *pool) refreshPool() {
	t := time.NewTimer(0)
	started := sync.Once{}
	for {
		select {
		case <-t.C:
			newEP, err := p.loadPool()
			if err == nil {
				toRemove := []string{}
				toAdd := []string{}

				p.lk.RLock()
				oldMap := make(map[string]bool)
				for _, o := range p.endpoints {
					oldMap[o.String()] = true
					remains := false
					for _, n := range newEP {
						if n == o.String() {
							remains = true
							break
						}
					}
					if !remains {
						toRemove = append(toRemove, o.String())
					}
				}
				p.lk.RUnlock()

				n := make([]Member, 0, len(newEP))
				for _, s := range newEP {
					n = append(n, Member(s))
					if _, ok := oldMap[s]; !ok {
						toAdd = append(toAdd, s)
					}
				}

				p.lk.Lock()
				if p.c == nil {
					ms := make([]consistent.Member, 0, len(toAdd))
					for _, a := range toAdd {
						ms = append(ms, Member(a))
					}
					p.c = consistent.New(ms, consistent.Config{
						PartitionCount:    701,
						ReplicationFactor: 20,
						Load:              1.25,
						Hasher:            hasher{},
					})
				} else {
					for _, a := range toAdd {
						p.c.Add(Member(a))
					}
					for _, r := range toRemove {
						p.c.Remove(r)
					}
				}
				p.endpoints = n
				p.lk.Unlock()
				poolSizeMetric.Set(float64(len(n)))
				started.Do(func() {
					close(p.started)
				})
			} else {
				poolErrorMetric.Add(1)
				fmt.Printf("error loading pool: %v\n", err)
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
	if left == 0 {
		left = DefaultMaxRetries
	}

	for i := 0; i < left; i++ {
		aff := with
		if aff == "" {
			aff = fmt.Sprintf("%d%s", i, c.Hash().B58String())
		} else {
			aff = fmt.Sprintf("%d%s", i, aff)
		}
		p.lk.RLock()
		member := p.c.LocateKey([]byte(aff))
		p.lk.RUnlock()
		root := member.String()

		blk, err = p.doFetch(ctx, root, c)
		if err != nil {
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
		goLogger.Infow("fetch result", "from", from, "of", c, "status", code, "size", received, "ttfb", int(fb.Sub(start).Milliseconds()), "duration", time.Since(start).Seconds())
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
	u, err := url.Parse(fmt.Sprintf(tmpl, from, c))
	if err != nil {
		return nil, err
	}
	req := http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/vnd.ipld.raw"},
		},
	}
	if p.config.ExtraHeaders != nil {
		for k, vs := range *p.config.ExtraHeaders {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	resp, err := p.config.Client.Do(&req)
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
			fmt.Printf("got %s vs %s\n", nc, c)
			return nil, blocks.ErrWrongHash
		}
	}
	return blocks.NewBlockWithCid(rb, c)
}
