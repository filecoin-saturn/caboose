package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
)

func TestPoolMirroring(t *testing.T) {
	if unsafe.Sizeof(unsafe.Pointer(nil)) <= 4 {
		t.Skip("skipping for 32bit architectures because too slow")
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

	carBytes := bytes.NewBuffer(nil)
	_, err := car.TraverseV1(context.TODO(), &ls, finalC, selectorparse.CommonSelector_MatchAllRecursively, carBytes)
	if err != nil {
		t.Fatal(err)
	}

	e := ep{}
	e.Setup()
	e.lk.Lock()
	e.carWrap = false
	e.resp = carBytes.Bytes()
	eURL := strings.TrimPrefix(e.server.URL, "https://")
	e.lk.Unlock()

	e2 := ep{}
	e2.Setup()
	e2.lk.Lock()
	e2.carWrap = false
	e2.resp = carBytes.Bytes()
	e2URL := strings.TrimPrefix(e2.server.URL, "https://")
	e2.lk.Unlock()

	conf := Config{
		OrchestratorEndpoint: &url.URL{},
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: []string{eURL, e2URL},
		LoggingEndpoint:      url.URL{},
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		Client:               saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Minute,
		MaxRetrievalAttempts: 1,
		MirrorFraction:       1.0,
	}

	p := newPool(&conf, nil)
	p.doRefresh()
	p.config.OrchestratorOverride = nil
	p.Start()

	// we don't know if any individual request is going to deterministically trigger a mirror request.
	// Make 10 requests, and expect some fraction trigger a mirror.

	for i := 0; i < 10; i++ {
		_, err = p.fetchBlockWith(context.Background(), finalC, "")
		if err != nil {
			t.Fatal(err)
		}

	}

	time.Sleep(100 * time.Millisecond)
	p.Close()

	e.lk.Lock()
	defer e.lk.Unlock()
	e2.lk.Lock()
	defer e2.lk.Unlock()
	if e.cnt+e2.cnt < 10 {
		t.Fatalf("expected at least 10 fetches, got %d", e.cnt+e2.cnt)
	}
}
