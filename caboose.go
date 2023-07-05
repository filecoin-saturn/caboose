package caboose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/filecoin-saturn/caboose/tieredhashing"

	ipfsblockstore "github.com/ipfs/boxo/blockstore"
	ipath "github.com/ipfs/boxo/coreiface/path"
	gateway "github.com/ipfs/boxo/gateway"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	SaturnEnvKey          = "STRN_ENV_TAG"
	OrchestratorJwtSecret = "JWT_SECRET"
)

type Config struct {
	// OrchestratorEndpoint is the URL of the Saturn orchestrator.
	OrchestratorEndpoint *url.URL
	// OrchestratorClient is the HTTP client to use when communicating with the Saturn orchestrator.
	OrchestratorClient *http.Client
	// OrchestratorOverride replaces calls to the orchestrator with a fixed response.
	OrchestratorOverride []tieredhashing.NodeInfo

	// OrchestratorJwtSecret is an auth secret that allows for Caboose to make authenticated
	// http requests to the orchestrator.
	OrchestratorJwtSecret string

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

	// MirrorFraction is what fraction of requests will be mirrored to another random node in order to track metrics / determine the current best nodes.
	MirrorFraction float64

	// MaxRetrievalAttempts determines the number of times we will attempt to retrieve a block from the Saturn network before failing.
	MaxRetrievalAttempts int

	// MaxFetchFailuresBeforeCoolDown is the maximum number of retrieval failures across the pool for a url before we auto-reject subsequent
	// fetches of that url.
	MaxFetchFailuresBeforeCoolDown int

	// FetchKeyCoolDownDuration is duration of time a key will stay in the cool down cache
	// before we start making retrieval attempts for it.
	FetchKeyCoolDownDuration time.Duration

	// SaturnNodeCoolOff is the cool off duration for a saturn node once we determine that we shouldn't be sending requests to it for a while.
	SaturnNodeCoolOff time.Duration

	TieredHashingOpts []tieredhashing.Option

	SentinelCidPeriod int64
}

const DefaultLoggingInterval = 5 * time.Second
const DefaultSaturnOrchestratorRequestTimeout = 30 * time.Second

const DefaultSaturnBlockRequestTimeout = 19 * time.Second
const DefaultSaturnCarRequestTimeout = 30 * time.Minute

// default retries before failure unless overridden by MaxRetrievalAttempts
const defaultMaxRetries = 3

// default percentage of requests to mirror for tracking how nodes perform unless overridden by MirrorFraction
const defaultMirrorFraction = 0.01

const maxBlockSize = 4194305 // 4 Mib + 1 byte
const DefaultOrchestratorEndpoint = "https://orchestrator.strn.pl/nodes?maxNodes=200"
const DefaultPoolRefreshInterval = 5 * time.Minute

// we cool off sending requests to Saturn for a cid for a certain duration
// if we've seen a certain number of failures for it already in a given duration.
// NOTE: before getting creative here, make sure you dont break end user flow
// described in https://github.com/ipni/storetheindex/pull/1344
const defaultMaxFetchFailures = 3 * defaultMaxRetries   // this has to fail more than DefaultMaxRetries done for a single gateway request
const defaultFetchKeyCoolDownDuration = 1 * time.Minute // how long will a sane person wait and stare at blank screen with "retry later" error before hitting F5?

// we cool off sending requests to a Saturn node if it returns transient errors rather than immediately downvoting it;
// however, only upto a certain max number of cool-offs.
const defaultSaturnNodeCoolOff = 5 * time.Minute

// This represents, on average, how many requests caboose makes before requesting a sentinel cid.
// Example: a period of 100 implies Caboose will on average make a sentinel CID request once every 100 requests.
const DefaultSentinelCidPeriod = int64(5)

var ErrNotImplemented error = errors.New("not implemented")
var ErrNoBackend error = errors.New("no available saturn backend")
var ErrContentProviderNotFound error = errors.New("saturn failed to find content providers")
var ErrSaturnTimeout error = errors.New("saturn backend timed out")

type ErrSaturnTooManyRequests struct {
	Node       string
	retryAfter time.Duration
}

func (e *ErrSaturnTooManyRequests) Error() string {
	return fmt.Sprintf("saturn node %s returned Too Many Requests error, please retry after %s", e.Node, humanRetry(e.retryAfter))
}

func (e *ErrSaturnTooManyRequests) RetryAfter() time.Duration {
	return e.retryAfter
}

type ErrCoolDown struct {
	Cid        cid.Cid
	Path       string
	retryAfter time.Duration
}

func (e *ErrCoolDown) Error() string {
	switch true {
	case e.Cid != cid.Undef && e.Path != "":
		return fmt.Sprintf("multiple saturn retrieval failures seen for CID %q and Path %q, please retry after %s", e.Cid, e.Path, humanRetry(e.retryAfter))
	case e.Path != "":
		return fmt.Sprintf("multiple saturn retrieval failures seen for Path %q, please retry after %s", e.Path, humanRetry(e.retryAfter))
	case e.Cid != cid.Undef:
		return fmt.Sprintf("multiple saturn retrieval failures seen for CID %q, please retry after %s", e.Cid, humanRetry(e.retryAfter))
	default:
		return fmt.Sprintf("multiple saturn retrieval failures for unknown CID/Path (BUG), please retry after %s", humanRetry(e.retryAfter))
	}
}

func (e *ErrCoolDown) RetryAfter() time.Duration {
	return e.retryAfter
}

func humanRetry(d time.Duration) string {
	return d.Truncate(time.Second).String()
}

// ErrPartialResponse can be returned from a DataCallback to indicate that some of the requested resource
// was successfully fetched, and that instead of retrying the full resource, that there are
// one or more more specific resources that should be fetched (via StillNeed) to complete the request.
type ErrPartialResponse struct {
	error
	StillNeed []string
}

func (epr ErrPartialResponse) Error() string {
	if epr.error != nil {
		return fmt.Sprintf("partial response: %s", epr.error.Error())
	}
	return "caboose received a partial response"
}

// ErrInvalidResponse can be returned from a DataCallback to indicate that the data provided for the
// requested resource was explicitly 'incorrect' - that blocks not in the requested dag, or non-car-conforming
// data was returned.
type ErrInvalidResponse error

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

	if config.SaturnNodeCoolOff == 0 {
		config.SaturnNodeCoolOff = defaultSaturnNodeCoolOff
	}
	if config.MirrorFraction == 0 {
		config.MirrorFraction = defaultMirrorFraction
	}
	if override := os.Getenv(BackendOverrideKey); len(override) > 0 {
		var overrideNodes []tieredhashing.NodeInfo
		err := json.Unmarshal([]byte(override), &overrideNodes)
		if err != nil {
			goLogger.Warnf("Error parsing BackendOverrideKey:", "err", err)
			return nil, err
		}
		config.OrchestratorOverride = overrideNodes
	}

	if jwtSecret := os.Getenv(OrchestratorJwtSecret); len(jwtSecret) > 0 {
		config.OrchestratorJwtSecret = jwtSecret
	}

	c := Caboose{
		config: config,
		pool:   newPool(config),
		logger: newLogger(config),
	}
	c.pool.logger = c.logger

	if c.config.SaturnClient == nil {
		c.config.SaturnClient = &http.Client{
			Timeout: DefaultSaturnCarRequestTimeout,
		}
	}
	if c.config.OrchestratorEndpoint == nil {
		var err error
		c.config.OrchestratorEndpoint, err = url.Parse(DefaultOrchestratorEndpoint)
		if err != nil {
			return nil, err
		}
	}

	if c.config.SentinelCidPeriod == 0 {
		c.config.SentinelCidPeriod = DefaultSentinelCidPeriod
	}

	if c.config.PoolRefresh == 0 {
		c.config.PoolRefresh = DefaultPoolRefreshInterval
	}

	if c.config.MaxRetrievalAttempts == 0 {
		c.config.MaxRetrievalAttempts = defaultMaxRetries
	}

	// start the pool
	c.pool.Start()

	return &c, nil
}

// Caboose is a blockstore.
var _ ipfsblockstore.Blockstore = (*Caboose)(nil)

func (c *Caboose) Close() {
	c.pool.Close()
	c.logger.Close()
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

// for testing only
func (c *Caboose) GetPoolPerf() map[string]*tieredhashing.NodePerf {
	return c.pool.th.GetPerf()
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
