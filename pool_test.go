package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.resp = testBlock
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	opts := []tieredhashing.Option{
		tieredhashing.WithCorrectnessWindowSize(1),
		tieredhashing.WithMaxMainTierSize(1),
		tieredhashing.WithAlwaysMainFirst(),
		tieredhashing.WithLatencyWindowSize(1),
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
	e.resp = carBytes.Bytes()
	eURL := strings.TrimPrefix(e.server.URL, "https://")

	e2 := ep{}
	e2.Setup()
	e2.resp = carBytes.Bytes()
	e2URL := strings.TrimPrefix(e2.server.URL, "https://")

	conf := Config{
		OrchestratorEndpoint: nil,
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: []string{eURL, e2URL},
		LoggingEndpoint:      url.URL{},
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		SaturnClient:         saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Millisecond * 50,
		MaxRetrievalAttempts: 1,
		TieredHashingOpts:    opts,
		MirrorFraction:       1.0,
	}

	p := newPool(&conf)
	p.Start()
	time.Sleep(time.Millisecond)
	// promote one node to main pool. other will remain in uknown pool.
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.UpdateMainTierWithTopN()
	fmt.Printf("metrics: %v\n", p.th.GetPoolMetrics())

	_, err = p.fetchBlockWith(context.Background(), finalC, "")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)
	p.Close()

	if e.cnt != 1 {
		t.Fatalf("expected 1 primary fetch, got %d", e.cnt)
	}
	if e2.cnt != 1 {
		t.Fatalf("expected 1 mirrored fetch, got %d", e2.cnt)
	}
}
