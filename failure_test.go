package caboose_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

var defaultCabooseWeight = 20

func TestCabooseTransientFailures(t *testing.T) {
	ctx := context.Background()
	ch := BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// All three nodes should return transient failures -> None get downvoted or removed
	// fetch fails
	ch.failNodesWithTransientErr(t, func(e *ep) bool {
		return true
	})
	require.EqualValues(t, 0, ch.nNodesAlive())
	_, err := ch.c.Get(ctx, testCid)
	require.Contains(t, err.Error(), "504")

	// run 50 fetches -> all nodes should still be in the ring
	ch.runFetchesForRandCids(50)
	require.EqualValues(t, 0, ch.nNodesAlive())
	require.EqualValues(t, 3, ch.getHashRingSize())

	weights := ch.getPoolWeights()
	require.Len(t, weights, 3)
	for _, w := range weights {
		require.EqualValues(t, defaultCabooseWeight, w)
	}

	// Only one node returns transient failure, it gets downvoted
	cnt := 0
	ch.recoverNodesFromTransientErr(t, func(e *ep) bool {
		if cnt < 2 {
			cnt++
			return true
		}
		return false
	})
	require.EqualValues(t, 2, ch.nNodesAlive())
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// assert node with transient failure is eventually downvoted
	ch.stopOrchestrator()
	i := 0
	require.Eventually(t, func() bool {
		randCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte{uint8(i)})
		i++
		_, _ = ch.c.Get(context.Background(), randCid)
		w := ch.getPoolWeights()
		for _, weight := range w {
			if weight < defaultCabooseWeight {
				return true
			}
		}
		return false

	}, 20*time.Second, 100*time.Millisecond)

	// but both the other nodes should have full weight
	weights = ch.getPoolWeights()
	cnt = 0

	for _, w := range weights {
		if w == defaultCabooseWeight {
			cnt++
		}
	}
	require.EqualValues(t, 2, cnt)
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
	require.EqualValues(t, 0, ch.getHashRingSize())

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
	require.Eventually(t, func() bool {
		return ch.getHashRingSize() == 3
	}, 10*time.Second, 100*time.Millisecond)
	ch.fetchAndAssertSuccess(t, ctx, testCid)
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
	coolDownErr, ok := err.(*caboose.ErrCidCoolDown)
	require.True(t, ok)
	require.EqualValues(t, cid, coolDownErr.Cid)
	require.NotZero(t, coolDownErr.RetryAfterMs)
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

func (ch *CabooseHarness) failNodesWithTransientErr(t *testing.T, selectorF func(ep *ep) bool) {
	for _, n := range ch.pool {
		if selectorF(n) {
			n.valid = false
			n.transientErr = true
		}
	}
}

func (ch *CabooseHarness) recoverNodesFromTransientErr(t *testing.T, selectorF func(ep *ep) bool) {
	for _, n := range ch.pool {
		if selectorF(n) {
			n.valid = true
			n.transientErr = false
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

func (ch *CabooseHarness) getHashRingSize() int {
	return len(ch.c.GetMemberWeights())
}

func (ch *CabooseHarness) getPoolWeights() map[string]int {
	return ch.c.GetMemberWeights()
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
	server         *httptest.Server
	valid          bool
	cnt            int
	tooManyReqsErr bool
	transientErr   bool
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.cnt++
		if e.valid {
			w.Write(testBlock)
		} else if e.transientErr {
			w.WriteHeader(http.StatusGatewayTimeout)
			w.Write([]byte("504"))
		} else if e.tooManyReqsErr {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("429"))
		} else {
			w.WriteHeader(503)
			w.Write([]byte("503"))
		}
	}))
}
