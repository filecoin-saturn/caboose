package caboose

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

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

	var ferr *ErrTooManyRequests
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
