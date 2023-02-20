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
	// OrchestratorEndpoint is the URL of the Saturn orchestrator.
	OrchestratorEndpoint *url.URL
	// OrchestratorClient is the HTTP client to use when communicating with the Saturn orchestrator.
	OrchestratorClient *http.Client

	// LoggingEndpoint is the URL of the logging endpoint where we submit logs pertaining to our Saturn retrieval requests.
	LoggingEndpoint url.URL
	// LoggingClient is the HTTP client to use when communicating with the logging endpoint.
	LoggingClient *http.Client
	// LoggingInterval is the interval at which we submit logs to the logging endpoint.
	LoggingInterval time.Duration

	// SaturnClient is the HTTP client to use when retrieving content from the Saturn network.
	SaturnClient *http.Client
	ExtraHeaders *http.Header

	// DoValidation is used to determine if we should validate the blocks recieved from the Saturn network.
	DoValidation bool
	// If set, AffinityKey is used instead of the block cid as the key on the Saturn node pool
	// to determine which Saturn node to retrieve the block from.
	AffinityKey string
	// PoolRefresh is the interval at which we refresh the pool of Saturn nodes.
	PoolRefresh time.Duration

	// PoolWeightChangeDebounce is the amount of time we wait between consecutive updates to the weight of a Saturn node
	// in our pool after a retrieval success/failure.
	PoolWeightChangeDebounce time.Duration

	// trigger early refreshes when pool size drops below this low watermark
	PoolLowWatermark int
	// MaxRetrievalAttempts determines the number of times we will attempt to retrieve a block from the Saturn network before failing.
	MaxRetrievalAttempts int
}

const DefaultMaxRetries = 3
const DefaultPoolFailureDownvoteDebounce = 2 * time.Second
const DefaultPoolLowWatermark = 5
const DefaultSaturnRequestTimeout = 19 * time.Second
const maxBlockSize = 4194305 // 4 Mib + 1 byte
const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes/nearby?count=1000"

var ErrNotImplemented error = errors.New("not implemented")
var ErrNoBackend error = errors.New("no available strn backend")
var ErrBackendFailed error = errors.New("strn backend failed")

// ErrTransient is returned when a transient error(timeouts, not found etc) occurs while fetching from the strn backend.
var ErrTransient error = errors.New("transient error while fetching from strn backend")

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
	if c.config.SaturnClient == nil {
		c.config.SaturnClient = &http.Client{
			Timeout: DefaultSaturnRequestTimeout,
		}
	}
	if c.config.OrchestratorEndpoint == nil {
		var err error
		c.config.OrchestratorEndpoint, err = url.Parse(DefaultOrchestratorEndpoint)
		if err != nil {
			return nil, err
		}
	}

	if c.config.PoolWeightChangeDebounce == 0 {
		c.config.PoolWeightChangeDebounce = DefaultPoolFailureDownvoteDebounce
	}
	if c.config.PoolLowWatermark == 0 {
		c.config.PoolLowWatermark = DefaultPoolLowWatermark
	}
	if c.config.MaxRetrievalAttempts == 0 {
		c.config.MaxRetrievalAttempts = DefaultMaxRetries
	}
	return &c, nil
}

func (c *Caboose) Close() {
	c.pool.Close()
	c.logger.Close()
}

// Note: Caboose is NOT a persistent blockstore and does NOT have an in-memory cache. Every block read request will escape to the Saturn network.
// Caching is left to the caller.

func (c *Caboose) Has(ctx context.Context, it cid.Cid) (bool, error) {
	blk, err := c.pool.fetchWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (c *Caboose) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	blk, err := c.pool.fetchWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return nil, err
	}
	return blk, nil
}

// GetSize returns the CIDs mapped BlockSize
func (c *Caboose) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	blk, err := c.pool.fetchWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

func (c *Caboose) getAffinity(ctx context.Context) string {
	if affC := ctx.Value(c.config.AffinityKey); affC != nil {
		return affC.(string)
	}
	return ""
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
