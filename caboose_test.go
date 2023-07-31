package caboose

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

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
	h := BuildCabooseHarness(t, 3, 3)

	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum(testBlock)

	h.fetchAndAssertSuccess(t, ctx, testCid)

	// ensure we have a success recording
	h.assertPoolSize(t, 3, 3)
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
	h.pool[0].carWrap = false

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
			return ErrPartialResponse{StillNeed: []string{"/path/to/car2"}}
		}
		if resource == "/path/to/car2" {
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
