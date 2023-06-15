package caboose_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/filecoin-saturn/caboose/tieredhashing"
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

func TestFetchBlock(t *testing.T) {
	ctx := context.Background()
	h := BuildCabooseHarness(t, 3, 3, WithTieredHashingOpts(
		[]tieredhashing.Option{tieredhashing.WithMaxMainTierSize(1), tieredhashing.WithCorrectnessWindowSize(2),
			tieredhashing.WithLatencyWindowSize(2)}))

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)

	h.fetchAndAssertSuccess(t, ctx, testCid)

	// ensure we have a success recording
	h.assertPoolSize(t, 0, 3, 3)
	h.assertCorrectnessCount(t, 1)
	h.assertLatencyCount(t, 1)

	h.fetchAndAssertSuccess(t, ctx, testCid)
	h.assertCorrectnessCount(t, 2)
	h.assertLatencyCount(t, 2)

	// all nodes fail
	h.failNodesWithCode(t, func(e *ep) bool {
		return true
	}, http.StatusNotAcceptable)

	h.fetchAndAssertFailure(t, ctx, testCid, "406")
}

func (h *CabooseHarness) assertLatencyCount(t *testing.T, expected int) {
	nds := h.c.GetPoolPerf()
	count := 0

	for _, perf := range nds {
		count += int(perf.NLatencyDigest)
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) assertCorrectnessCount(t *testing.T, expected int) {
	nds := h.c.GetPoolPerf()
	count := 0

	for _, perf := range nds {
		count += int(perf.NCorrectnessDigest)
	}
	require.EqualValues(t, expected, count)
}

func (h *CabooseHarness) assertPoolSize(t *testing.T, mainS, unknownS, totalS int) {
	nds := h.c.GetPoolPerf()
	require.Equal(t, totalS, len(nds))

	var eMain int
	var eUnknown int

	for _, perf := range nds {
		if perf.Tier == "main" {
			eMain++
		}
		if perf.Tier == "unknown" {
			eUnknown++
		}
	}

	require.EqualValues(t, eMain, mainS)
	require.EqualValues(t, eUnknown, unknownS)
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
	lnk := ls.MustStore(linking.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, n)
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

	// confirm that errors propogate.
	if err := h.c.Fetch(context.Background(), "/path/to/car", func(resource string, reader io.Reader) error {
		return fmt.Errorf("test error")
	}); err.Error() != "test error" {
		t.Fatalf("expected error. got %v", err)
	}

	// confirm partial failures work as expected.
	second := false
	if err := h.c.Fetch(context.Background(), "/path/to/car1", func(resource string, reader io.Reader) error {
		if resource == "/path/to/car1" {
			return caboose.ErrPartialResponse{StillNeed: []string{"/path/to/car2"}}
		}
		if resource == "/path/to/car2" {
			fmt.Printf("doing second...\n")
			second = true
			return nil
		}
		t.Fatal("unexpected call")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !second {
		t.Fatal("expected fall-over progress")
	}
}

type HarnessOption func(config *caboose.Config)

func WithTieredHashingOpts(opts []tieredhashing.Option) HarnessOption {
	return func(config *caboose.Config) {
		config.TieredHashingOpts = opts
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

func BuildCabooseHarness(t *testing.T, n int, maxRetries int, opts ...HarnessOption) *CabooseHarness {
	ch := &CabooseHarness{}

	ch.pool = make([]*ep, n)
	purls := make([]tieredhashing.NodeInfo, n)
	for i := 0; i < len(ch.pool); i++ {
		ch.pool[i] = &ep{}
		ch.pool[i].Setup()
		ip := strings.TrimPrefix(ch.pool[i].server.URL, "https://")
		purls[i] = tieredhashing.NodeInfo{
			IP: ip,
			ID: "node-id",
			Weight: rand.Intn(100),
			Distance: rand.Float32(),
			SentinelCid: "sentinel-cid",
		}
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

		SaturnClient:         saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Millisecond * 50,
		MaxRetrievalAttempts: maxRetries,
	}

	for _, opt := range opts {
		opt(conf)
	}

	bs, err := caboose.NewCaboose(conf)
	require.NoError(t, err)

	ch.c = bs
	return ch
}
