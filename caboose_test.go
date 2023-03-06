package caboose_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestCidCoolDown(t *testing.T) {
	ctx := context.Background()
	ch := BuildCabooseHarness(t, 3, 3, WithMaxFailuresBeforeCoolDown(2), WithCidCoolDownDuration(1*time.Second))

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)
	ch.fetchAndAssertSuccess(t, ctx, testCid)

	// Invalidate all servers so we cool down cids
	ch.failNodesWithCode(t, func(e *ep) bool {
		return true
	}, 503)

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

func WithPoolMembershipDebounce(d time.Duration) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.PoolMembershipDebounce = d
	}
}

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

func WithMaxNCoolOff(n int) func(config *caboose.Config) {
	return func(config *caboose.Config) {
		config.MaxNCoolOff = n
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

	ch.c = bs
	return ch
}

func TestResource(t *testing.T) {
	h := BuildCabooseHarness(t, 1, 3)
	// some setup.
	buf := bytes.NewBuffer(nil)
	ls := cidlink.DefaultLinkSystem()
	store := memstore.Store{}
	ls.SetReadStorage(&store)
	ls.SetWriteStorage(&store)
	n := basicnode.NewBytes(testBlock)
	lnk := ls.MustStore(linking.LinkContext{}, cidlink.LinkPrototype{cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, n)
	rt := lnk.(cidlink.Link).Cid

	// make our carv1
	car.TraverseV1(context.Background(), &ls, rt, selectorparse.CommonSelector_MatchPoint, buf)
	h.pool[0].resp = buf.Bytes()

	// ask for it.
	if err := h.c.Fetch(context.Background(), "/path/to/car", func(resource string, reader io.Reader) error {
		if resource != "/path/to/car" {
			t.Fatal("incorrect path in resource callback")
		}
		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("couldn't read: %v", err)
		}
		if !bytes.Equal(got, buf.Bytes()) {
			t.Fatal("unexpected response")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
