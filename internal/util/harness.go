package util

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/filecoin-saturn/caboose/internal/state"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func BuildCabooseHarness(t *testing.T, n int, maxRetries int, opts ...HarnessOption) *CabooseHarness {
	ch := &CabooseHarness{}

	ch.Endpoints = make([]*Endpoint, n)
	purls := make([]string, n)
	for i := 0; i < len(ch.Endpoints); i++ {
		ch.Endpoints[i] = &Endpoint{}
		ch.Endpoints[i].Setup()
		purls[i] = strings.TrimPrefix(ch.Endpoints[i].Server.URL, "https://")
	}
	ch.goodOrch = true
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.gol.Lock()
		defer ch.gol.Unlock()
		if ch.goodOrch {
			json.NewEncoder(w).Encode(purls)
		} else {
			json.NewEncoder(w).Encode([]string{})
		}
	}))

	saturnClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         "example.com",
			},
		},
	}

	ourl, _ := url.Parse(orch.URL)

	conf := &caboose.Config{
		OrchestratorEndpoint: ourl,
		OrchestratorClient:   http.DefaultClient,
		LoggingEndpoint:      *ourl,
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		Client:               saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Millisecond * 50,
		MaxRetrievalAttempts: maxRetries,
		Harness:              &state.State{},
	}

	for _, opt := range opts {
		opt(conf)
	}

	bs, err := caboose.NewCaboose(conf)
	require.NoError(t, err)

	ch.Caboose = bs
	ch.CabooseActiveNodes = conf.Harness.ActiveNodes.(*caboose.NodeRing)
	ch.CabooseAllNodes = conf.Harness.AllNodes.(*caboose.NodeHeap)
	return ch
}

type CabooseHarness struct {
	Caboose   *caboose.Caboose
	Endpoints []*Endpoint

	CabooseActiveNodes *caboose.NodeRing
	CabooseAllNodes    *caboose.NodeHeap

	gol      sync.Mutex
	goodOrch bool
}

type NodeStats struct {
	Start time.Time
	Latency float64
	Size float64
}

func (ch *CabooseHarness) RunFetchesForRandCids(n int) {
	for i := 0; i < n; i++ {
		randCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte{uint8(i)})
		_, _ = ch.Caboose.Get(context.Background(), randCid)
	}
}

func (ch *CabooseHarness) FetchAndAssertCoolDownError(t *testing.T, ctx context.Context, cid cid.Cid) {
	_, err := ch.Caboose.Get(ctx, cid)
	require.Error(t, err)

	var coolDownErr *caboose.ErrCoolDown
	ok := errors.As(err, &coolDownErr)
	require.True(t, ok)
	require.Contains(t, coolDownErr.Path, cid.String())
	require.NotZero(t, coolDownErr.RetryAfter())
}

func (ch *CabooseHarness) FetchAndAssertFailure(t *testing.T, ctx context.Context, testCid cid.Cid, contains string) {
	_, err := ch.Caboose.Get(ctx, testCid)
	require.Error(t, err)
	require.Contains(t, err.Error(), contains)
}

func (ch *CabooseHarness) FetchAndAssertSuccess(t *testing.T, ctx context.Context, c cid.Cid) {
	blk, err := ch.Caboose.Get(ctx, c)
	require.NoError(t, err)
	require.NotEmpty(t, blk)
}

func (ch *CabooseHarness) RecordSuccesses(t *testing.T, nodes []*caboose.Node, s NodeStats, n int) {
	for _, node := range(nodes) {
		s.Start = time.Now().Add(-time.Second*5)
		for i := 0; i < n; i++ {
			node.RecordSuccess(s.Start, s.Latency, s.Size)
		}
	}
}

func (ch *CabooseHarness) RecordFailures(t *testing.T, nodes []*caboose.Node, n int) {
	for _, node := range(nodes) {
		for i := 0; i < n; i++ {
			node.RecordFailure()
		}
	}
}


func (ch *CabooseHarness) FailNodesWithCode(t *testing.T, selectorF func(ep *Endpoint) bool, code int) {
	for _, n := range ch.Endpoints {
		if selectorF(n) {
			n.Valid = false
			n.httpCode = code
		}
	}
}

func (ch *CabooseHarness) RecoverNodes(t *testing.T, selectorF func(ep *Endpoint) bool) {
	for _, n := range ch.Endpoints {
		if selectorF(n) {
			n.Valid = true
		}
	}
}

func (ch *CabooseHarness) FailNodesAndAssertFetch(t *testing.T, selectorF func(ep *Endpoint) bool, nAlive int, cid cid.Cid) {
	ch.FailNodes(t, selectorF)
	require.EqualValues(t, nAlive, ch.NNodesAlive())
	ch.FetchAndAssertSuccess(t, context.Background(), cid)
}

func (ch *CabooseHarness) FailNodes(t *testing.T, selectorF func(ep *Endpoint) bool) {
	for _, n := range ch.Endpoints {
		if selectorF(n) {
			n.Valid = false
		}
	}
}

func (ch *CabooseHarness) NNodesAlive() int {
	cnt := 0
	for _, n := range ch.Endpoints {
		if n.Valid {
			cnt++
		}
	}
	return cnt
}

func (ch *CabooseHarness) StopOrchestrator() {
	ch.gol.Lock()
	ch.goodOrch = false
	ch.gol.Unlock()
}

func (ch *CabooseHarness) StartOrchestrator() {
	ch.gol.Lock()
	ch.goodOrch = true
	ch.gol.Unlock()
}

func (h *CabooseHarness) AssertLatencyCount(t *testing.T, expected int) {
	nds := h.CabooseActiveNodes
	count := 0

	ndl := nds.Len()
	nodes, err := nds.GetNodes("", ndl)
	require.NoError(t, err, "Getting nodes should succeed")
	for _, perf := range nodes {
		perf.Samples.Each(func(_ caboose.NodeSample) {
			count += 1
		})
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) AssertCorrectnessCount(t *testing.T, expected int) {
	nds := h.CabooseActiveNodes
	count := 0

	ndl := nds.Len()
	nodes, err := nds.GetNodes("", ndl)
	require.NoError(t, err, "Getting nodes should succeed")
	for _, perf := range nodes {
		perf.Samples.Each(func(_ caboose.NodeSample) {
			count += 1
		})
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) AssertPoolSize(t *testing.T, activeS, totalS int) {
	active := h.CabooseActiveNodes.Len()
	all := h.CabooseAllNodes.Len()

	require.Equal(t, totalS, all)
	require.Equal(t, activeS, active)
}

type HarnessOption func(config *caboose.Config)

func WithMaxFailuresBeforeCoolDown(max int) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.MaxFetchFailuresBeforeCoolDown = max
	}
}

func WithCidCoolDownDuration(duration time.Duration) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.FetchKeyCoolDownDuration = duration
	}
}

type Endpoint struct {
	Server   *httptest.Server
	Valid    bool
	count    int
	httpCode int
	Resp     []byte
	CarWrap  bool
	lk       sync.Mutex
}

var testBlock = []byte("hello World")

func (e *Endpoint) Setup() {
	e.Valid = true
	e.CarWrap = true
	e.Resp = testBlock
	e.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.lk.Lock()
		defer e.lk.Unlock()
		e.count++
		if e.Valid {
			if e.CarWrap {
				c, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(e.Resp)

				car.WriteHeader(&car.CarHeader{
					Roots:   []cid.Cid{c},
					Version: 1,
				}, w)
				util.LdWrite(w, c.Bytes(), e.Resp)
			} else {
				w.Write(e.Resp)
			}
		} else {
			if e.httpCode == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "1")
			}
			if e.httpCode == 0 {
				e.httpCode = 500
			}
			w.WriteHeader(e.httpCode)
			w.Write([]byte("error"))
		}
	}))
}

func (ep *Endpoint) SetResp(resp []byte, wrap bool) {
	ep.lk.Lock()
	defer ep.lk.Unlock()
	ep.Resp = resp
	ep.CarWrap = wrap
}

func (ep *Endpoint) Count() int {
	ep.lk.Lock()
	defer ep.lk.Unlock()
	return ep.count
}
