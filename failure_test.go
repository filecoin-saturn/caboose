package caboose_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

var expRetryAfter = 1 * time.Second

func TestHttp429(t *testing.T) {
	ctx := context.Background()
	ch := BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.failNodesWithCode(t, func(e *ep) bool {
		return true
	}, http.StatusTooManyRequests)

	_, err := ch.c.Get(ctx, testCid)
	require.Error(t, err)

	var ferr *caboose.ErrSaturnTooManyRequests
	ok := errors.As(err, &ferr)
	require.True(t, ok)
	require.EqualValues(t, expRetryAfter, ferr.RetryAfter())
}

func TestCabooseFailures(t *testing.T) {
	ctx := context.Background()
	ch := BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// fail primary
	ch.failNodesAndAssertFetch(t, func(e *ep) bool {
		return e.cnt > 0 && e.valid
	}, 2, testCid)

	// fail primary and secondary.
	ch.failNodesAndAssertFetch(t, func(e *ep) bool {
		return e.cnt > 0 && e.valid
	}, 1, testCid)

	// force pool down to the 1 remaining good node.
	ch.stopOrchestrator()
	ch.runFetchesForRandCids(50)
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// invalidate ALL nodes
	ch.failNodes(t, func(ep *ep) bool {
		return true
	})
	ch.runFetchesForRandCids(50)
	require.EqualValues(t, 0, ch.nNodesAlive())

	_, err := ch.c.Get(context.Background(), testCid)
	require.Error(t, err)

	// more nodes should populate
	ch.startOrchestrator()
	cnt := 0
	ch.recoverNodes(t, func(ep *ep) bool {
		if cnt == 0 {
			cnt++
			return true
		}
		return false
	})
	time.Sleep(time.Millisecond * 100)

	//steady state-ify
	ch.runFetchesForRandCids(50)
	_, err = ch.c.Get(context.Background(), testCid)
	require.NoError(t, err)
}

type CabooseHarness struct {
	c    *caboose.Caboose
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

	var coolDownErr *caboose.ErrCoolDown
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

type ep struct {
	server   *httptest.Server
	valid    bool
	cnt      int
	httpCode int
	resp     []byte
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.resp = testBlock
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond * 20)
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
