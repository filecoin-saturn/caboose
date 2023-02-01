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
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// loadPool refreshes the set of endpoints to fetch cars from from the Orchestrator Endpoint
func (p *pool) loadPool() ([]string, error) {
	resp, err := p.config.OrchestratorClient.Get(p.config.OrchestratorEndpoint.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responses := make([]string, 0)
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		return nil, err
	}
	return responses, nil
}

type pool struct {
	config    *Config
	endpoints []Member
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
				started.Do(func() {
					close(p.started)
				})
			} else {
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

func (p *pool) fetchWith(ctx context.Context, c cid.Cid, with string) (blocks.Block, error) {
	<-p.started
	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}
	p.lk.RLock()
	member := p.c.LocateKey([]byte(aff))
	p.lk.RUnlock()
	root := member.String()

	return p.doFetch(ctx, root, c)
}

var tmpl = "http://%s/ipfs/%s?format=raw"

func (p *pool) doFetch(ctx context.Context, from string, c cid.Cid) (blocks.Block, error) {
	u, err := url.Parse(fmt.Sprintf(tmpl, from, c))
	if err != nil {
		return nil, err
	}
	resp, err := p.config.Client.Do(&http.Request{
		Method: http.MethodGet,
		URL:    u,
		Header: http.Header{
			"Accept": []string{"application/vnd.ipld.raw"},
		},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

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
