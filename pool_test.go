package caboose_test

import (
	"bytes"
	"context"
	"testing"
	"time"
	"unsafe"

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

	data := []byte("hello World")
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

	ch := util.BuildCabooseHarness(t, 2, 3)

	if err != nil {
		t.Fatal(err)
	}

	// we don't know if any individual request is going to deterministically trigger a mirror request.
	// Make 10 requests, and expect some fraction trigger a mirror.

	for i := 0; i < 10; i++ {
		_, err = ch.Caboose.Get(context.Background(), finalC)
		if err != nil {
			t.Fatal(err)
		}

	}

	time.Sleep(100 * time.Millisecond)
	ch.Caboose.Close()

	ec := ch.Endpoints[0].Count()

	e2c := ch.Endpoints[1].Count()
	if ec+e2c < 10 {
		t.Fatalf("expected at least 10 fetches, got %d", ec+e2c)
	}
}

func TestFetchComplianceCid(t *testing.T) {
	if unsafe.Sizeof(unsafe.Pointer(nil)) <= 4 {
		t.Skip("skipping for 32bit architectures because too slow")
	}

	ch := util.BuildCabooseHarness(t, 1, 1, util.WithComplianceCidPeriod(1), util.WithMirrorFraction(1.0))

	ch.CaboosePool.DoRefresh()

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid

	_, err := ch.Caboose.Get(context.Background(), finalC)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	ch.Caboose.Close()

	e := ch.Endpoints[0]

	if e.Count() != 2 {
		t.Fatalf("expected 2 primary fetch, got %d", e.Count())
	}
}
