package caboose

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

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

type CabooseHarness struct {
	c    *Caboose
	pool []*ep

	gol      sync.Mutex
	goodOrch bool
}

func (ch *CabooseHarness) runFetchesForRandCids(n int) {
	for i := 0; i < n; i++ {
		randCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte{uint8(i)})
		_, _ = ch.c.Get(context.Background(), randCid)
	}
}

func (ch *CabooseHarness) fetchAndAssertCoolDownError(t *testing.T, ctx context.Context, cid cid.Cid) {
	_, err := ch.c.Get(ctx, cid)
	require.Error(t, err)

	var coolDownErr *ErrCoolDown
	ok := errors.As(err, &coolDownErr)
	require.True(t, ok)
	require.EqualValues(t, cid, coolDownErr.Cid)
	require.NotZero(t, coolDownErr.RetryAfter())
}

func (ch *CabooseHarness) fetchAndAssertFailure(t *testing.T, ctx context.Context, testCid cid.Cid, contains string) {
	_, err := ch.c.Get(ctx, testCid)
	require.Error(t, err)
	require.Contains(t, err.Error(), contains)
}

func (ch *CabooseHarness) fetchAndAssertSuccess(t *testing.T, ctx context.Context, c cid.Cid) {
	blk, err := ch.c.Get(ctx, c)
	require.NoError(t, err)
	require.NotEmpty(t, blk)
}
func (ch *CabooseHarness) failNodesWithCode(t *testing.T, selectorF func(ep *ep) bool, code int) {
	for _, n := range ch.pool {
		if selectorF(n) {
			n.valid = false
			n.httpCode = code
		}
	}
}

func (ch *CabooseHarness) recoverNodes(t *testing.T, selectorF func(ep *ep) bool) {
	for _, n := range ch.pool {
		if selectorF(n) {
			n.valid = true
		}
	}
}

func (ch *CabooseHarness) failNodesAndAssertFetch(t *testing.T, selectorF func(ep *ep) bool, nAlive int, cid cid.Cid) {
	ch.failNodes(t, selectorF)
	require.EqualValues(t, nAlive, ch.nNodesAlive())
	ch.fetchAndAssertSuccess(t, context.Background(), cid)
}

func (ch *CabooseHarness) failNodes(t *testing.T, selectorF func(ep *ep) bool) {
	for _, n := range ch.pool {
		if selectorF(n) {
			n.valid = false
		}
	}
}

func (ch *CabooseHarness) nNodesAlive() int {
	cnt := 0
	for _, n := range ch.pool {
		if n.valid {
			cnt++
		}
	}
	return cnt
}

func (ch *CabooseHarness) stopOrchestrator() {
	ch.gol.Lock()
	ch.goodOrch = false
	ch.gol.Unlock()
}

func (ch *CabooseHarness) startOrchestrator() {
	ch.gol.Lock()
	ch.goodOrch = true
	ch.gol.Unlock()
}

func (h *CabooseHarness) assertLatencyCount(t *testing.T, expected int) {
	nds := h.c.pool.ActiveNodes
	count := 0

	for _, perf := range nds.nodes {
		perf.Samples.Each(func(_ NodeSample) {
			count += 1
		})
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) assertCorrectnessCount(t *testing.T, expected int) {
	nds := h.c.pool.ActiveNodes
	count := 0

	for _, perf := range nds.nodes {
		perf.Samples.Each(func(_ NodeSample) {
			count += 1
		})
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) assertPoolSize(t *testing.T, activeS, totalS int) {
	pool := h.c.pool
	active := len(pool.ActiveNodes.nodes)
	all := pool.AllNodes.Len()

	require.Equal(t, totalS, all)
	require.Equal(t, activeS, active)
}

type HarnessOption func(config *Config)

func WithMaxFailuresBeforeCoolDown(max int) func(config *Config) {
	return func(config *Config) {
		config.MaxFetchFailuresBeforeCoolDown = max
	}
}

func WithCidCoolDownDuration(duration time.Duration) func(config *Config) {
	return func(config *Config) {
		config.FetchKeyCoolDownDuration = duration
	}
}

func BuildCabooseHarness(t *testing.T, n int, maxRetries int, opts ...HarnessOption) *CabooseHarness {
	ch := &CabooseHarness{}

	ch.pool = make([]*ep, n)
	purls := make([]string, n)
	for i := 0; i < len(ch.pool); i++ {
		ch.pool[i] = &ep{}
		ch.pool[i].Setup()
		purls[i] = strings.TrimPrefix(ch.pool[i].server.URL, "https://")
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

	conf := &Config{
		OrchestratorEndpoint: ourl,
		OrchestratorClient:   http.DefaultClient,
		LoggingEndpoint:      *ourl,
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		Client:               saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Millisecond * 50,
		MaxRetrievalAttempts: maxRetries,
	}

	for _, opt := range opts {
		opt(conf)
	}

	bs, err := NewCaboose(conf)
	require.NoError(t, err)

	ch.c = bs
	return ch
}

type ep struct {
	server   *httptest.Server
	valid    bool
	cnt      int
	httpCode int
	resp     []byte
	lk       sync.Mutex
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.resp = testBlock
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.lk.Lock()
		defer e.lk.Unlock()
		e.cnt++
		if e.valid {
			w.Write(e.resp)
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
