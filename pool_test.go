package caboose_test

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

	"github.com/filecoin-saturn/caboose"
	"github.com/filecoin-saturn/caboose/internal/util"
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

	e := util.Endpoint{}
	e.Setup()
	e.SetResp(carBytes.Bytes(), false)
	eURL := strings.TrimPrefix(e.Server.URL, "https://")

	e2 := util.Endpoint{}
	e2.Setup()
	e2.SetResp(carBytes.Bytes(), false)
	e2URL := strings.TrimPrefix(e2.Server.URL, "https://")

	conf := caboose.Config{
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

	c, err := caboose.NewCaboose(&conf)
	if err != nil {
		t.Fatal(err)
	}

	// we don't know if any individual request is going to deterministically trigger a mirror request.
	// Make 10 requests, and expect some fraction trigger a mirror.

	for i := 0; i < 10; i++ {
		_, err = c.Get(context.Background(), finalC)
		if err != nil {
			t.Fatal(err)
		}

	}

	time.Sleep(100 * time.Millisecond)
	c.Close()

	ec := e.Count()
	e2c := e2.Count()
	if ec+e2c < 10 {
		t.Fatalf("expected at least 10 fetches, got %d", ec+e2c)
	}
}
