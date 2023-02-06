package cache

import (
	"context"
	"errors"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	ipfsblockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"
)

type ContextCache struct {
	backing ipfsblockstore.Blockstore

	ctxkey string
	cache  sync.Map
}

var (
	ErrNotImplemented error = errors.New("not implemented")
	ErrInternalError  error = errors.New("internal error")
)

func NewContextCachingBlockstore(backing ipfsblockstore.Blockstore, ctxkey string) (ipfsblockstore.Blockstore, error) {
	c := ContextCache{
		backing: backing,
		ctxkey:  ctxkey,
	}
	return &c, nil
}

func (c *ContextCache) Has(ctx context.Context, it cid.Cid) (bool, error) {
	var ap *lru.ARCCache
	rcache, loaded := c.cache.LoadOrStore(ctx.Value(c.ctxkey), &ap)
	if loaded {
		// alocate underlying cache for this request.
		act, err := lru.NewARC(1024)
		if err != nil {
			return false, err
		}
		ap = act
		go c.cleanup(ctx)
	}
	cp, ok := rcache.(**lru.ARCCache)
	if !ok {
		// this can be a race. may want to pause and retry access to the pointer.
		return false, ErrInternalError
	}
	reqCache := *cp
	if v, ok := reqCache.Get(it); ok {
		return v != nil, nil
	}

	// write-through
	blk, err := c.backing.Get(ctx, it)
	if err != nil {
		return false, err
	}
	reqCache.Add(it, blk)
	return blk != nil, nil
}

func (c *ContextCache) cleanup(ctx context.Context) {
	val := ctx.Value(c.ctxkey)
	<-ctx.Done()
	c.cache.Delete(val)
}

func (c *ContextCache) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	var ap *lru.ARCCache
	rcache, loaded := c.cache.LoadOrStore(ctx.Value(c.ctxkey), &ap)
	if loaded {
		// alocate underlying cache for this request.
		act, err := lru.NewARC(1024)
		if err != nil {
			return nil, err
		}
		ap = act
		go c.cleanup(ctx)
	}
	cp, ok := rcache.(**lru.ARCCache)
	if !ok {
		// this can be a race. may want to pause and retry access to the pointer.
		return nil, ErrInternalError
	}
	reqCache := *cp
	if v, ok := reqCache.Get(it); ok {
		return v.(blocks.Block), nil
	}

	// write-through
	blk, err := c.backing.Get(ctx, it)
	if err != nil {
		return nil, err
	}
	reqCache.Add(it, blk)
	return blk, nil
}

// GetSize returns the CIDs mapped BlockSize
func (c *ContextCache) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	var ap *lru.ARCCache
	rcache, loaded := c.cache.LoadOrStore(ctx.Value(c.ctxkey), &ap)
	if loaded {
		// alocate underlying cache for this request.
		act, err := lru.NewARC(1024)
		if err != nil {
			return 0, err
		}
		ap = act
		go c.cleanup(ctx)
	}
	cp, ok := rcache.(**lru.ARCCache)
	if !ok {
		// this can be a race. may want to pause and retry access to the pointer.
		return 0, ErrInternalError
	}
	reqCache := *cp
	if v, ok := reqCache.Get(it); ok {
		return len(v.(blocks.Block).RawData()), nil
	}

	// write-through
	blk, err := c.backing.Get(ctx, it)
	if err != nil {
		return 0, err
	}
	reqCache.Add(it, blk)
	return len(blk.RawData()), nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (c *ContextCache) HashOnRead(enabled bool) {
	c.backing.HashOnRead(enabled)
}

/* Mutable blockstore methods */
func (c *ContextCache) Put(context.Context, blocks.Block) error {
	return ErrNotImplemented
}

func (c *ContextCache) PutMany(context.Context, []blocks.Block) error {
	return ErrNotImplemented
}
func (c *ContextCache) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, ErrNotImplemented
}
func (c *ContextCache) DeleteBlock(context.Context, cid.Cid) error {
	return ErrNotImplemented
}

var _ ipfsblockstore.Blockstore = (*ContextCache)(nil)
