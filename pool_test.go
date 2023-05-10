package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
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
	opts := []tieredhashing.Option{tieredhashing.WithCorrectnessWindowSize(1), tieredhashing.WithMaxPoolSize(5)}

	saturnClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	conf := Config{
		OrchestratorEndpoint: nil,
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: []string{},
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

	conf.OrchestratorOverride = []string{e.server.URL}

	urlWOScheme := strings.TrimPrefix(e.server.URL, "https://")
	_, _, err = p.doFetch(context.Background(), urlWOScheme, finalC, 1)
	if err != nil {
		t.Fatal(err)
	}

	p.Close()

	if e.cnt != 2 {
		t.Fatalf("expected 2 fetches with mirroring, got %d", e.cnt)
	}
}
