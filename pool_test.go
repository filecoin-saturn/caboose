package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
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

	saturnClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	data := []byte("hello world")
	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(data))
	finalC := finalCL.(cidlink.Link).Cid
	cw, err := car.NewSelectiveWriter(context.TODO(), &ls, finalC, selectorparse.CommonSelector_MatchAllRecursively)
	if err != nil {
		t.Fatal(err)
	}
	carBytes := bytes.NewBuffer(nil)
	cw.WriteTo(carBytes)

	e := ep{}
	e.Setup()
	e.lk.Lock()
	e.resp = carBytes.Bytes()
	eURL := strings.TrimPrefix(e.server.URL, "https://")
	eNodeInfo := tieredhashing.NodeInfo {
		IP: eURL,
		ID: eURL,
		Weight: rand.Intn(100),
		Distance: rand.Float32(),
		SentinelCid: "node1",
	}
	e.lk.Unlock()

	e2 := ep{}
	e2.Setup()
	e2.lk.Lock()
	e2.resp = carBytes.Bytes()
	e2URL := strings.TrimPrefix(e2.server.URL, "https://")
	e2NodeInfo := tieredhashing.NodeInfo {
		IP: e2URL,
		ID: e2URL,
		Weight: rand.Intn(100),
		Distance: rand.Float32(),
		SentinelCid: "node2",

	}
	e2.lk.Unlock()

	conf := Config{
		OrchestratorEndpoint: &url.URL{},
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: []tieredhashing.NodeInfo{eNodeInfo, e2NodeInfo},
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

	p := newPool(&conf)
	p.doRefresh()
	p.config.OrchestratorOverride = nil
	p.Start()

	// promote one node to main pool. other will remain in uknown pool.
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.UpdateMainTierWithTopN()

	_, err = p.fetchBlockWith(context.Background(), finalC, "")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	p.Close()

	e.lk.Lock()
	defer e.lk.Unlock()
	if e.cnt != 1 {
		t.Fatalf("expected 1 primary fetch, got %d", e.cnt)
	}
	e2.lk.Lock()
	defer e2.lk.Unlock()
	if e2.cnt != 1 {
		t.Fatalf("expected 1 mirrored fetch, got %d", e2.cnt)
	}
}
