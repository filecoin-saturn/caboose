package caboose

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/ipfs/go-cid"
	ipfsblockstore "github.com/ipfs/go-ipfs-blockstore"
	blocks "github.com/ipfs/go-libipfs/blocks"
)

type Config struct {
	OrchestratorEndpoint url.URL
	OrchestratorClient   *http.Client

	LoggingEndpoint url.URL
	LoggingClient   *http.Client
	LoggingInterval time.Duration

	Client       *http.Client
	ExtraHeaders *http.Header

	DoValidation   bool
	AffinityKey    string
	PoolRefresh    time.Duration
	PoolMaxSize    int
	MaxConcurrency int
	MaxRetries     int
}

const DefaultMaxRetries = 3

var ErrNotImplemented error = errors.New("not implemented")

type Caboose struct {
	config *Config
	pool   *pool
	logger *logger
}

func NewCaboose(config *Config) (ipfsblockstore.Blockstore, error) {
	c := Caboose{
		config: config,
		pool:   newPool(config),
		logger: newLogger(config),
	}
	c.pool.logger = c.logger
	if c.config.Client == nil {
		c.config.Client = http.DefaultClient
	}
	return &c, nil
}

func (c *Caboose) Close() {
	c.pool.Close()
	c.logger.Close()
}

func (c *Caboose) Has(ctx context.Context, it cid.Cid) (bool, error) {
	aff := ctx.Value(c.config.AffinityKey)
	if aff != nil {
		blk, err := c.pool.fetchWith(ctx, it, aff.(string))
		if err != nil {
			return false, err
		}
		return blk != nil, nil
	}
	blk, err := c.pool.fetchWith(ctx, it, "")
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (c *Caboose) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	aff := ctx.Value(c.config.AffinityKey)
	if aff != nil {
		blk, err := c.pool.fetchWith(ctx, it, aff.(string))
		if err != nil {
			return nil, err
		}
		return blk, nil
	}
	blk, err := c.pool.fetchWith(ctx, it, "")
	if err != nil {
		return nil, err
	}
	return blk, nil
}

// GetSize returns the CIDs mapped BlockSize
func (c *Caboose) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	aff := ctx.Value(c.config.AffinityKey)
	if aff != nil {
		blk, err := c.pool.fetchWith(ctx, it, aff.(string))
		if err != nil {
			return 0, err
		}
		return len(blk.RawData()), nil
	}
	blk, err := c.pool.fetchWith(ctx, it, "")
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

// HashOnRead specifies if every read block should be
// rehashed to make sure it matches its CID.
func (c *Caboose) HashOnRead(enabled bool) {
	c.config.DoValidation = enabled
}

/* Mutable blockstore methods */
func (c *Caboose) Put(context.Context, blocks.Block) error {
	return ErrNotImplemented
}

func (c *Caboose) PutMany(context.Context, []blocks.Block) error {
	return ErrNotImplemented
}
func (c *Caboose) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, ErrNotImplemented
}
func (c *Caboose) DeleteBlock(context.Context, cid.Cid) error {
	return ErrNotImplemented
}

var _ ipfsblockstore.Blockstore = (*Caboose)(nil)
