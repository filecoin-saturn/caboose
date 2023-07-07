package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/filecoin-saturn/caboose/tieredhashing"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
)

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

func TestPoolMiroring(t *testing.T) {
	if unsafe.Sizeof(unsafe.Pointer(nil)) <= 4 {
		t.Skip("skipping for 32bit architectures because too slow")
	}
	opts := []tieredhashing.Option{
		tieredhashing.WithCorrectnessWindowSize(2),
		tieredhashing.WithLatencyWindowSize(2),
		tieredhashing.WithMaxMainTierSize(1),
	}
	ph := BuildPoolHarness(t, 2, opts)

	p := ph.p
	p.config.SentinelCidPeriod = 0
	nodes := ph.p.config.OrchestratorOverride
	p.doRefresh()
	p.config.OrchestratorOverride = nil
	p.Start()

	// promote one node to main pool. other will remain in uknown pool.
	eURL := nodes[0].IP
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.UpdateMainTierWithTopN()

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid

	_, err := p.fetchBlockWith(context.Background(), finalC, "")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	p.Close()

	for _, e := range ph.eps {
		e.lk.Lock()
		defer e.lk.Unlock()
		if e.cnt != 1 {
			t.Fatalf("expected 1 primary fetch, got %d", e.cnt)
		}
	}
}

func TestLoadPool(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte("node"))
		response := [1]tieredhashing.NodeInfo{{
			IP:          "node",
			ID:          "node",
			Weight:      rand.Intn(100),
			Distance:    rand.Float32(),
			SentinelCid: cid.String(),
		}}

		w.Header().Set("Content-Type", "application/json")

		// Encoding the response to JSON
		json.NewEncoder(w).Encode(response)
	}))

	endpoint, _ := url.Parse(server.URL)
	p := &pool{
		config: &Config{
			OrchestratorEndpoint:  endpoint,
			OrchestratorClient:    http.DefaultClient,
		},
	}

	_, err := p.loadPool()

	assert.NoError(t, err)
}

func TestFetchSentinelCid(t *testing.T) {
	if unsafe.Sizeof(unsafe.Pointer(nil)) <= 4 {
		t.Skip("skipping for 32bit architectures because too slow")
	}
	opts := []tieredhashing.Option{
		tieredhashing.WithCorrectnessWindowSize(2),
		tieredhashing.WithLatencyWindowSize(2),
		tieredhashing.WithMaxMainTierSize(1),
	}
	ph := BuildPoolHarness(t, 2, opts)

	p := ph.p
	p.config.SentinelCidPeriod = 1
	nodes := ph.p.config.OrchestratorOverride
	p.doRefresh()
	p.config.OrchestratorOverride = nil
	p.Start()

	// promote one node to main pool. other will remain in uknown pool.
	eURL := nodes[0].IP
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.UpdateMainTierWithTopN()

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid

	_, err := p.fetchBlockWith(context.Background(), finalC, "")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	p.Close()

	e := ph.eps[1]
	e.lk.Lock()
	defer e.lk.Unlock()

	if e.cnt != 2 {
		t.Fatalf("expected 2 primary fetch, got %d", e.cnt)
	}
}

type PoolHarness struct {
	p   *pool
	eps []*ep
}

func BuildPoolHarness(t *testing.T, n int, opts []tieredhashing.Option) *PoolHarness {

	saturnClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid
	cw, err := car.NewSelectiveWriter(context.TODO(), &ls, finalC, selectorparse.CommonSelector_MatchAllRecursively)
	if err != nil {
		t.Fatal(err)
	}
	carBytes := bytes.NewBuffer(nil)
	cw.WriteTo(carBytes)

	nodeInfos := make([]tieredhashing.NodeInfo, n)
	eps := make([]*ep, n)

	for i := 0; i < n; i++ {
		eps[i] = &ep{}
		eps[i].Setup()
		eps[i].lk.Lock()
		eps[i].resp = carBytes.Bytes()
		eURL := strings.TrimPrefix(eps[i].server.URL, "https://")
		nodeInfos[i] = tieredhashing.NodeInfo{
			IP:          eURL,
			ID:          eURL,
			Weight:      rand.Intn(100),
			Distance:    rand.Float32(),
			SentinelCid: finalC.String(),
		}
		eps[i].lk.Unlock()

	}

	conf := Config{
		OrchestratorEndpoint: &url.URL{},
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: nodeInfos,
		LoggingEndpoint:      url.URL{},
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,
		SaturnClient:         saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Minute,
		MaxRetrievalAttempts: 1,
		TieredHashingOpts:    opts,
		MirrorFraction:       1.0,
	}

	ph := &PoolHarness{
		p:   newPool(&conf),
		eps: eps,
	}
	return ph
}
