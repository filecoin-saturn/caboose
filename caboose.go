package caboose

import (
	"context"
	"errors"
	"fmt"
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

	// MaxCidFailuresBeforeCoolDown is the maximum number of cid retrieval failures across the pool we will tolerate before we
	// add the cid to the cool down cache.
	MaxCidFailuresBeforeCoolDown int

	// CidCoolDownDuration is duration of time a cid will stay in the cool down cache
	// before we start making retrieval attempts for it.
	CidCoolDownDuration time.Duration

	// SaturnNodeCoolOff is the cool off duration for a saturn node once we determine that we shouldn't be sending requests to it for a while.
	SaturnNodeCoolOff time.Duration

	// MaxNCoolOff is the number of times we will cool off a node before downvoting it.
	MaxNCoolOff int
}

const DefaultMaxRetries = 3
const DefaultPoolFailureDownvoteDebounce = 1 * time.Minute
const DefaultPoolMembershipDebounce = 3 * DefaultPoolRefreshInterval
const DefaultPoolLowWatermark = 5
const DefaultSaturnRequestTimeout = 19 * time.Second
const maxBlockSize = 4194305 // 4 Mib + 1 byte
const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes/nearby?count=1000"
const DefaultPoolRefreshInterval = 5 * time.Minute

// we cool off sending requests to Saturn for a cid for a certain duration
// if we've seen a certain number of failures for it already in a given duration.
// NOTE: before getting creative here, make sure you dont break end user flow
// described in https://github.com/ipni/storetheindex/pull/1344
const DefaultMaxCidFailures = 3 * DefaultMaxRetries // this has to fail more than DefaultMaxRetries done for a single gateway request
const DefaultCidCoolDownDuration = 1 * time.Minute  // how long will a sane person wait and stare at blank screen with "retry later" error before hitting F5?

// we cool off sending requests to a Saturn node if it returns transient errors rather than immediately downvoting it;
// however, only upto a certain max number of cool-offs.
const DefaultSaturnNodeCoolOff = 5 * time.Minute
const DefaultMaxNCoolOff = 3

var ErrNotImplemented error = errors.New("not implemented")
var ErrNoBackend error = errors.New("no available saturn backend")
var ErrBackendFailed error = errors.New("saturn backend failed")
var ErrContentProviderNotFound error = errors.New("saturn failed to find content providers")
var ErrSaturnTimeout error = errors.New("saturn backend timed out")

type ErrSaturnTooManyRequests struct {
	Node       string
	RetryAfter time.Duration // TODO: DRY refactor after https://github.com/ipfs/go-libipfs/issues/188
}

func (e ErrSaturnTooManyRequests) Error() string {
	return fmt.Sprintf("saturn node %s returned Too Many Requests error, please retry after %s", e.Node, humanRetry(e.RetryAfter))
}

type ErrCidCoolDown struct {
	Cid        cid.Cid
	RetryAfter time.Duration // TODO: DRY refactor after https://github.com/ipfs/go-libipfs/issues/188
}

func (e *ErrCidCoolDown) Error() string {
	return fmt.Sprintf("multiple saturn retrieval failures seen for CID %s, please retry after %s", e.Cid, humanRetry(e.RetryAfter))
}

// TODO: move this to upstream error interface in https://github.com/ipfs/go-libipfs/issues/188
// and refactor ErrCidCoolDown and ErrSaturnTooManyRequests to inherit from that instead
func humanRetry(d time.Duration) string {
	return d.Truncate(time.Second).String()
}

type Caboose struct {
	config *Config
	pool   *pool
	logger *logger
}

func NewCaboose(config *Config) (ipfsblockstore.Blockstore, error) {

	if config.CidCoolDownDuration == 0 {
		config.CidCoolDownDuration = DefaultCidCoolDownDuration
	}
	if config.MaxCidFailuresBeforeCoolDown == 0 {
		config.MaxCidFailuresBeforeCoolDown = DefaultMaxCidFailures
	}

	if config.SaturnNodeCoolOff == 0 {
		config.SaturnNodeCoolOff = DefaultSaturnNodeCoolOff
	}

	if config.MaxNCoolOff == 0 {
		config.MaxNCoolOff = DefaultMaxNCoolOff
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
