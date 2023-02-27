package caboose_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestCidCoolDown(t *testing.T) {
	ctx := context.Background()
	ch := BuildCabooseHarness(t, 3, 3, WithMaxCidFailuresBeforeCoolDown(2), WithCidCoolDownDuration(1*time.Second))

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// Invalidate all servers so we cool down cids
	ch.failNodes(t, func(e *ep) bool {
		return true
	})

	// Fetch should fail with fetch error
	ch.fetchAndAssertFailure(t, ctx, testCid, "503")

	// second fetch should fail with fetch error
	ch.fetchAndAssertFailure(t, ctx, testCid, "503")

	// next fetch should fail with cool down error
	ch.fetchAndAssertCoolDownError(t, ctx, testCid)

	// one more fetch should fail with cool down error
	ch.fetchAndAssertCoolDownError(t, ctx, testCid)

	ch.recoverNodes(t, func(e *ep) bool {
		return true
	})

	// fetch should eventually succeed once cid is removed from the cool down cache
	require.Eventually(t, func() bool {
		_, err := ch.c.Get(ctx, testCid)
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)
}

type HarnessOption func(config *caboose.Config)

func WithMaxCidFailuresBeforeCoolDown(max int) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.MaxCidFailuresBeforeCoolDown = max
	}
}

func WithCidCoolDownDuration(duration time.Duration) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.CidCoolDownDuration = duration
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

	conf := &caboose.Config{
		OrchestratorEndpoint: ourl,
		OrchestratorClient:   http.DefaultClient,
		LoggingEndpoint:      *ourl,
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		SaturnClient:             saturnClient,
		DoValidation:             false,
		PoolWeightChangeDebounce: time.Duration(1),
		PoolRefresh:              time.Millisecond * 50,
		MaxRetrievalAttempts:     maxRetries,
		PoolMembershipDebounce:   1,
	}

	for _, opt := range opts {
		opt(conf)
	}

	bs, err := caboose.NewCaboose(conf)
	require.NoError(t, err)

	ch.c = bs.(*caboose.Caboose)
	return ch
}
