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
	gateway "github.com/ipfs/go-libipfs/gateway"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
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

	// If set, AffinityKey is used instead of the block CID as the key on the
	// Saturn node pool to determine which Saturn node to retrieve the block from.
	// NOTE: If gateway.ContentPathKey is present in request context,
	// it will be used as AffinityKey automatically.
	AffinityKey string

	// PoolRefresh is the interval at which we refresh the pool of Saturn nodes.
	PoolRefresh time.Duration

	// PoolWeightChangeDebounce is the amount of time we wait between consecutive updates to the weight of a Saturn node
	// in our pool after a retrieval success/failure.
	PoolWeightChangeDebounce time.Duration

	// PoolMembershipDebounce is the amount of time we wait after a saturn node is removed from the pool
	// before we add it again to the pool.
	PoolMembershipDebounce time.Duration

	// trigger early refreshes when pool size drops below this low watermark
	PoolLowWatermark int
	// MaxRetrievalAttempts determines the number of times we will attempt to retrieve a block from the Saturn network before failing.
	MaxRetrievalAttempts int

	// TooManyReqsCoolOff is the cool of duration for a saturn node once it returns a 429
	TooManyReqsCoolOff time.Duration
}

const DefaultMaxRetries = 3
const DefaultPoolFailureDownvoteDebounce = time.Second
const DefaultPoolMembershipDebounce = 5 * time.Minute
const DefaultPoolLowWatermark = 5
const DefaultSaturnRequestTimeout = 19 * time.Second
const DefaultSaturnGlobalBlockFetchTimeout = 60 * time.Second
const maxBlockSize = 4194305 // 4 Mib + 1 byte
const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes/nearby?count=1000"
const DefaultPoolRefreshInterval = 5 * time.Minute
const DefaultTooManyReqsCoolOff = 5 * time.Minute
const MaxNCoolOff = 3

var ErrNotImplemented error = errors.New("not implemented")
var ErrNoBackend error = errors.New("no available strn backend")
var ErrBackendFailed error = errors.New("strn backend failed")
var ErrContentProviderNotFound error = errors.New("strn failed to find content providers")
var ErrSaturnTimeout error = errors.New("strn backend timed out")
var ErrSaturnTooManyRequests error = errors.New("strn backend returned too many requests error; 429")

type Caboose struct {
	config *Config
	pool   *pool
	logger *logger
}

func NewCaboose(config *Config) (ipfsblockstore.Blockstore, error) {
	if config.TooManyReqsCoolOff == 0 {
		config.TooManyReqsCoolOff = DefaultTooManyReqsCoolOff
	}
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

	if c.config.PoolRefresh == 0 {
		c.config.PoolRefresh = DefaultPoolRefreshInterval
	}

	if c.config.PoolWeightChangeDebounce == 0 {
		c.config.PoolWeightChangeDebounce = DefaultPoolFailureDownvoteDebounce
	}
	if c.config.PoolMembershipDebounce == 0 {
		c.config.PoolMembershipDebounce = DefaultPoolMembershipDebounce
	}
	if c.config.PoolLowWatermark == 0 {
		c.config.PoolLowWatermark = DefaultPoolLowWatermark
	}
	if c.config.MaxRetrievalAttempts == 0 {
		c.config.MaxRetrievalAttempts = DefaultMaxRetries
	}

	// start the pool
	c.pool.Start()

	return &c, nil
}

// GetMemberWeights is for testing ONLY
func (c *Caboose) GetMemberWeights() map[string]int {
	c.pool.lk.RLock()
	defer c.pool.lk.RUnlock()

	return c.pool.endpoints.ToWeights()
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
	// https://github.com/ipfs/bifrost-gateway/issues/53#issuecomment-1442732865
	if affG := ctx.Value(gateway.ContentPathKey); affG != nil {
		contentPath := affG.(ipath.Path).String()

		// Using exact content path seems to work better for initial website loads
		// because it groups all blocks related to the single file,
		// but at the same time spreads files across multiple L1s
		// which removes the risk of specific L1 becoming a cache hot spot for
		// websites with huge DAG like /ipns/en.wikipedia-on-ipfs.org
		return contentPath

		/* TODO: if we ever want to revisit, and group per root CID of entire DAG:
		const contentRootIdx = 2
		if parts := strings.Split(contentPath, "/"); len(parts) > contentRootIdx {
			// use top level contentRoot ('id' from /ipfs/id or /ipns/id) as affinity key
			return parts[contentRootIdx]
		}
		*/
	}
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
