// Stoker loads a dag from a remote host into an in-memory blockstore with the
// intention that it will be processed in a traversal-order compatible way such
// that data can be efficiently streamed and not fully buffered in memory.
package stoker

import (
	"context"
	"io"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipld/go-car"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	ipfsblockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"
	golog "github.com/ipfs/go-log/v2"
)

var goLogger = golog.Logger("caboose/stoker")

type Stoker struct {
	*caboose.Caboose
}

func New(c *caboose.Caboose) *Stoker {
	return &Stoker{c}
}

const SessionCacheSize = 1024

func (s *Stoker) NewSession(ctx context.Context, path string) ipfsblockstore.Blockstore {
	cache, err := lru.NewARC(SessionCacheSize)
	if err != nil {
		goLogger.Warnw("failed to allocate cache for session", "err", err)
		return nil
	}

	ss := stokerSession{
		c:                      s.Caboose,
		cache:                  cache,
		writeAheadSynchronizer: NewWAS(SessionCacheSize*3/4, SessionCacheSize*1/4),
	}
	go ss.fillCache(ctx, path)

	return &ss
}

type stokerSession struct {
	c     *caboose.Caboose
	cache *lru.ARCCache
	*writeAheadSynchronizer
}

func (ss *stokerSession) fillCache(ctx context.Context, path string) error {
	err := ss.c.Fetch(ctx, path, func(resource string, reader io.Reader) error {
		cr, err := car.NewCarReader(reader)
		if err != nil {
			return err
		}

		n := 0
		for {
			ss.writeAheadSynchronizer.ReqAdd()
			blk, err := cr.Next()
			if err == nil {
				n++
				ss.cache.Add(blk.Cid(), blk)
			} else {
				// for now, fall back to blocks on partial
				if n > 0 {
					return caboose.ErrPartialResponse{}
				}
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	})
	return err
}

func (ss *stokerSession) Has(ctx context.Context, it cid.Cid) (bool, error) {
	if v, ok := ss.cache.Get(it); ok {
		return v != nil, nil
	}

	blk, err := ss.getUncached(ctx, it)
	return blk != nil, err
}

func (ss *stokerSession) getUncached(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	blk, err := ss.c.Get(ctx, it)
	if err != nil {
		return nil, err
	}
	ss.cache.Add(it, blk)
	return blk, nil
}

func (ss *stokerSession) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	if cached, ok := ss.cache.Get(it); ok {
		// todo: maybe we should only do this on reads against the cids coming
		// from pre-fetching. that said, this will more likely read out the
		// fill pre-fetch stream, which is still a reasonable choice.
		ss.writeAheadSynchronizer.Dec()

		return cached.(blocks.Block), nil
	}

	return ss.getUncached(ctx, it)
}

// GetSize returns the CIDs mapped BlockSize
func (ss *stokerSession) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	if cached, ok := ss.cache.Get(it); ok {
		blk := cached.(blocks.Block)
		return len(blk.RawData()), nil
	}

	blk, err := ss.getUncached(ctx, it)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (ss *stokerSession) HashOnRead(enabled bool) {
}

/* Mutable blockstore methods */
func (ss *stokerSession) Put(context.Context, blocks.Block) error {
	return caboose.ErrNotImplemented
}

func (ss *stokerSession) PutMany(context.Context, []blocks.Block) error {
	return caboose.ErrNotImplemented
}
func (ss *stokerSession) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, caboose.ErrNotImplemented
}
func (ss *stokerSession) DeleteBlock(context.Context, cid.Cid) error {
	return caboose.ErrNotImplemented
}
