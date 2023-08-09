package caboose_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/filecoin-saturn/caboose/internal/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

var expRetryAfter = 1 * time.Second

func TestHttp429(t *testing.T) {
	ctx := context.Background()
	ch := util.BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.FailNodesWithCode(t, func(e *util.Endpoint) bool {
		return true
	}, http.StatusTooManyRequests)

	_, err := ch.Caboose.Get(ctx, testCid)
	require.Error(t, err)

	var ferr *caboose.ErrTooManyRequests
	ok := errors.As(err, &ferr)
	require.True(t, ok)
	require.EqualValues(t, expRetryAfter, ferr.RetryAfter())
}

func TestCabooseFailures(t *testing.T) {
	ctx := context.Background()
	ch := util.BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.FetchAndAssertSuccess(t, ctx, testCid)

	// fail primary
	ch.FailNodesAndAssertFetch(t, func(e *util.Endpoint) bool {
		return e.Count() > 0 && e.Valid
	}, 2, testCid)

	// fail primary and secondary.
	ch.FailNodesAndAssertFetch(t, func(e *util.Endpoint) bool {
		return e.Count() > 0 && e.Valid
	}, 1, testCid)

	// force pool down to the 1 remaining good node.
	ch.StopOrchestrator()
	ch.RunFetchesForRandCids(50)
	ch.FetchAndAssertSuccess(t, ctx, testCid)

	// invalidate ALL nodes
	ch.FailNodes(t, func(ep *util.Endpoint) bool {
		return true
	})
	ch.RunFetchesForRandCids(50)
	require.EqualValues(t, 0, ch.NNodesAlive())

	_, err := ch.Caboose.Get(context.Background(), testCid)
	require.Error(t, err)

	// more nodes should populate
	ch.StartOrchestrator()
	cnt := 0
	ch.RecoverNodes(t, func(ep *util.Endpoint) bool {
		if cnt == 0 {
			cnt++
			return true
		}
		return false
	})
	time.Sleep(time.Millisecond * 100)

	//steady state-ify
	ch.RunFetchesForRandCids(50)
	_, err = ch.Caboose.Get(context.Background(), testCid)
	require.NoError(t, err)
}
