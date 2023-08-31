package caboose

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	ipfsblockstore "github.com/ipfs/boxo/blockstore"
	ipath "github.com/ipfs/boxo/coreiface/path"
	gateway "github.com/ipfs/boxo/gateway"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/filecoin-saturn/caboose/internal/state"
)

const (
	BackendOverrideKey = "CABOOSE_BACKEND_OVERRIDE"
)

type Config struct {
	// OrchestratorEndpoint is the URL for fetching upstream nodes.
	OrchestratorEndpoint *url.URL
	// OrchestratorClient is the HTTP client to use when communicating with the orchestrator.
	OrchestratorClient *http.Client
	// OrchestratorOverride replaces calls to the orchestrator with a fixed response.
	OrchestratorOverride []string

	// LoggingEndpoint is the URL of the logging endpoint where we submit logs pertaining to retrieval requests.
	LoggingEndpoint url.URL
	// LoggingClient is the HTTP client to use when communicating with the logging endpoint.
	LoggingClient *http.Client
	// LoggingInterval is the interval at which we submit logs to the logging endpoint.
	LoggingInterval time.Duration

	// Client is the HTTP client to use when retrieving content from upstream nodes.
	Client       *http.Client
	ExtraHeaders *http.Header

	// DoValidation is used to determine if we should validate the blocks recieved from the upstream.
	DoValidation bool

	// If set, AffinityKey is used instead of the block CID as the key on the
	// pool to determine which upstream to retrieve the request from.
	// NOTE: If gateway.ContentPathKey is present in request context,
	// it will be used as AffinityKey automatically.
	AffinityKey string

	// PoolRefresh is the interval at which we refresh the pool of upstreams from the orchestrator.
	PoolRefresh time.Duration

	// MirrorFraction is what fraction of requests will be mirrored to another random node in order to track metrics / determine the current best nodes.
	MirrorFraction float64

	// MaxRetrievalAttempts determines the number of times we will attempt to retrieve a block from upstreams before failing.
	MaxRetrievalAttempts int

	// MaxFetchFailuresBeforeCoolDown is the maximum number of retrieval failures across the pool for a url before we auto-reject subsequent
	// fetches of that url.
	MaxFetchFailuresBeforeCoolDown int

	// FetchKeyCoolDownDuration is duration of time a key will stay in the cool down cache
	// before we start making retrieval attempts for it.
	FetchKeyCoolDownDuration time.Duration

	// CoolOff is the cool off duration for a node once we determine that we shouldn't be sending requests to it for a while.
	CoolOff time.Duration

	// Harness is an internal test harness that is set during testing.
	Harness *state.State
}

const DefaultLoggingInterval = 5 * time.Second
const DefaultOrchestratorRequestTimeout = 30 * time.Second

const DefaultBlockRequestTimeout = 19 * time.Second
const DefaultCarRequestTimeout = 30 * time.Minute

// default retries before failure unless overridden by MaxRetrievalAttempts
const defaultMaxRetries = 3

// default percentage of requests to mirror for tracking how nodes perform unless overridden by MirrorFraction
const defaultMirrorFraction = 0.01

const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes/nearby?count=200"
const DefaultPoolRefreshInterval = 5 * time.Minute

// we cool off sending requests for a cid for a certain duration
// if we've seen a certain number of failures for it already in a given duration.
// NOTE: before getting creative here, make sure you dont break end user flow
// described in https://github.com/ipni/storetheindex/pull/1344
const defaultMaxFetchFailures = 3 * defaultMaxRetries   // this has to fail more than DefaultMaxRetries done for a single gateway request
const defaultFetchKeyCoolDownDuration = 1 * time.Minute // how long will a sane person wait and stare at blank screen with "retry later" error before hitting F5?

// we cool off sending requests to a node if it returns transient errors rather than immediately downvoting it;
// however, only upto a certain max number of cool-offs.
const defaultNodeCoolOff = 5 * time.Minute

type Caboose struct {
	config *Config
	pool   *pool
	logger *logger
}

// DataCallback allows for extensible validation of path-retrieved data.
type DataCallback func(resource string, reader io.Reader) error

// NewCaboose sets up a caboose fetcher.
// Note: Caboose is NOT a persistent blockstore and does NOT have an in-memory cache.
// Every request will result in a remote network request.
func NewCaboose(config *Config) (*Caboose, error) {
	if config.FetchKeyCoolDownDuration == 0 {
		config.FetchKeyCoolDownDuration = defaultFetchKeyCoolDownDuration
	}
	if config.MaxFetchFailuresBeforeCoolDown == 0 {
		config.MaxFetchFailuresBeforeCoolDown = defaultMaxFetchFailures
	}

	if config.CoolOff == 0 {
		config.CoolOff = defaultNodeCoolOff
	}
	if config.MirrorFraction == 0 {
		config.MirrorFraction = defaultMirrorFraction
	}
	if override := os.Getenv(BackendOverrideKey); len(override) > 0 {
		config.OrchestratorOverride = strings.Split(override, ",")
	}

	logger := newLogger(config)
	c := Caboose{
		config: config,
		pool:   newPool(config, logger),
		logger: logger,
	}

	if c.config.Client == nil {
		c.config.Client = &http.Client{
			Timeout: DefaultCarRequestTimeout,
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

	if c.config.MaxRetrievalAttempts == 0 {
		c.config.MaxRetrievalAttempts = defaultMaxRetries
	}

	// Set during testing to leak internal state to the harness.
	if c.config.Harness != nil {
		c.config.Harness.ActiveNodes = c.pool.ActiveNodes
		c.config.Harness.AllNodes = c.pool.AllNodes
		c.config.Harness.PoolController = c.pool
	}

	// start the pool
	c.pool.Start()

	return &c, nil
}

// Caboose is a blockstore.
var _ ipfsblockstore.Blockstore = (*Caboose)(nil)

func (c *Caboose) Close() {
	c.pool.Close()
	if c.logger != nil {
		c.logger.Close()
	}
}

// Fetch allows fetching car archives by a path of the form `/ipfs/<cid>[/path/to/file]`
func (c *Caboose) Fetch(ctx context.Context, path string, cb DataCallback) error {
	ctx, span := spanTrace(ctx, "Fetch", trace.WithAttributes(attribute.String("path", path)))
	defer span.End()

	return c.pool.fetchResourceWith(ctx, path, cb, c.getAffinity(ctx))
}

func (c *Caboose) Has(ctx context.Context, it cid.Cid) (bool, error) {
	ctx, span := spanTrace(ctx, "Has", trace.WithAttributes(attribute.Stringer("cid", it)))
	defer span.End()

	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return false, err
	}
	return blk != nil, nil
}

func (c *Caboose) Get(ctx context.Context, it cid.Cid) (blocks.Block, error) {
	ctx, span := spanTrace(ctx, "Get", trace.WithAttributes(attribute.Stringer("cid", it)))
	defer span.End()

	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
	if err != nil {
		return nil, err
	}
	return blk, nil
}

// GetSize returns the CIDs mapped BlockSize
func (c *Caboose) GetSize(ctx context.Context, it cid.Cid) (int, error) {
	ctx, span := spanTrace(ctx, "GetSize", trace.WithAttributes(attribute.Stringer("cid", it)))
	defer span.End()

	blk, err := c.pool.fetchBlockWith(ctx, it, c.getAffinity(ctx))
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
