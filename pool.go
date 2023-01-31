package caboose

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// loadPool refreshes the set of endpoints to fetch cars from from the Orchestrator Endpoint
func (p *pool) loadPool() ([]string, error) {
	resp, err := p.config.Client.Get(p.config.OrchestratorEndpoint.String())
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
		config: c,
		c: consistent.New([]consistent.Member{}, consistent.Config{
			PartitionCount:    7,
			ReplicationFactor: 20,
			Load:              1.25,
			Hasher:            hasher{},
		}),
		done: make(chan struct{}, 1),
	}
	go p.refreshPool()
	return &p
}

func (p *pool) refreshPool() {
	t := time.NewTimer(p.config.PoolRefresh)
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
				for _, a := range toAdd {
					p.c.Add(Member(a))
				}
				for _, r := range toRemove {
					p.c.Remove(r)
				}
				p.endpoints = n
				p.lk.Unlock()
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
	aff := with
	if aff == "" {
		aff = c.Hash().B58String()
	}
	member := p.c.LocateKey([]byte(aff))
	root := member.String()

	return doFetch(ctx, p.config.Client, root, c)
}

var tmpl = "http://%s/ipfs/%s?format=raw"

func doFetch(ctx context.Context, client *http.Client, from string, c cid.Cid) (blocks.Block, error) {
	u := fmt.Sprintf(tmpl, from, c)
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(rb, c)
}
