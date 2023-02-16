package caboose_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

func TestCabooseFailures(t *testing.T) {

	pool := make([]ep, 3)
	purls := make([]string, 3)
	for i := 0; i < len(pool); i++ {
		pool[i].Setup()
		purls[i] = strings.TrimPrefix(pool[i].server.URL, "https://")
	}
	gol := sync.Mutex{}
	goodOrch := true
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gol.Lock()
		defer gol.Unlock()
		if goodOrch {
			json.NewEncoder(w).Encode(purls)
		} else {
			json.NewEncoder(w).Encode([]string{})
		}
	}))

	ourl, _ := url.Parse(orch.URL)
	c, err := caboose.NewCaboose(&caboose.Config{
		OrchestratorEndpoint: ourl,
		OrchestratorClient:   http.DefaultClient,
		LoggingEndpoint:      *ourl,
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,

		SaturnClient:             http.DefaultClient,
		DoValidation:             false,
		PoolWeightChangeDebounce: time.Duration(1),
		PoolRefresh:              time.Millisecond * 50,
		MaxRetrievalAttempts:     2,
	})
	if err != nil {
		t.Fatal(err)
	}

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)

	_, err = c.Get(context.Background(), testCid)
	if err != nil {
		t.Fatal(err)
	}

	// fail primary.
	cnt := 0
	for i := 0; i < len(pool); i++ {
		if pool[i].cnt > 0 {
			pool[i].valid = false
			cnt++
		}
	}
	if cnt != 1 {
		t.Fatalf("should have invalidated 1 backend. actually invalidated %d", cnt)
	}

	_, err = c.Get(context.Background(), testCid)
	if err != nil {
		t.Fatal(err)
	}

	// fail primary and secondary. should get error.
	cnt = 0
	for i := 0; i < len(pool); i++ {
		if pool[i].cnt > 0 && pool[i].valid {
			pool[i].valid = false
			cnt++
		}
	}
	if cnt != 1 {
		t.Fatalf("should have invalidated 1 more backend. actually invalidated %d", cnt)
	}

	// force pool down to the 1 remaining good node.
	gol.Lock()
	goodOrch = false
	gol.Unlock()
	for i := 0; i < 20; i++ {
		randCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte{uint8(i)})
		c.Get(context.Background(), randCid)
	}

	_, err = c.Get(context.Background(), testCid)
	if err != nil {
		t.Fatalf("we should still have the good backend. got %v", err)
	}

	// pool empty state should error.
	for i := 0; i < len(pool); i++ {
		pool[i].valid = false
	}
	for i := 0; i < 20; i++ {
		c.Get(context.Background(), testCid)
	}
	_, err = c.Get(context.Background(), testCid)
	if err == nil {
		t.Fatal("we should have no backends")
	}

	// more nodes should populate
	gol.Lock()
	goodOrch = true
	gol.Unlock()
	pool[0].valid = true
	time.Sleep(time.Millisecond * 100)

	//steady state-ify
	for i := 0; i < 20; i++ {
		randCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte{uint8(i)})
		c.Get(context.Background(), randCid)
	}

	_, err = c.Get(context.Background(), testCid)
	if err != nil {
		t.Fatalf("we should get backends again. got %d", err)
	}

}

type ep struct {
	server *httptest.Server
	valid  bool
	cnt    int
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.cnt++
		if e.valid {
			w.Write(testBlock)
		} else {
			w.WriteHeader(503)
			w.Write([]byte("503"))
		}
	}))
}
