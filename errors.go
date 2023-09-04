package caboose

import (
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
)

// ErrNotImplemented is used when using caboose as a blockstore and attempting to mutate data.
var ErrNotImplemented error = errors.New("not implemented")

// ErrNoBackend is returned when no connection can be made to learn of available backends.
var ErrNoBackend error = errors.New("no available backend")

// ErrContentProviderNotFound is used to indicate that data could not be found upstream.
var ErrContentProviderNotFound error = errors.New("failed to find data")

// ErrTimeout is used to indicate that attempts to find data timed out.
var ErrTimeout error = errors.New("upstream timed out")

// ErrTooManyRequests indicates that the client has been rate limited by upstreams.
type ErrTooManyRequests struct {
	Node       string
	retryAfter time.Duration
}

func (e *ErrTooManyRequests) Error() string {
	return fmt.Sprintf("upstream %s returned Too Many Requests error, please retry after %s", e.Node, humanRetry(e.retryAfter))
}

func (e *ErrTooManyRequests) RetryAfter() time.Duration {
	return e.retryAfter
}

// ErrCoolDown indicates that the requested CID has been requested too many times recently.
type ErrCoolDown struct {
	Cid        cid.Cid
	Path       string
	retryAfter time.Duration
}

func (e *ErrCoolDown) Error() string {
	switch true {
	case e.Cid != cid.Undef && e.Path != "":
		return fmt.Sprintf("multiple retrieval failures seen for CID %q and Path %q, please retry after %s", e.Cid, e.Path, humanRetry(e.retryAfter))
	case e.Path != "":
		return fmt.Sprintf("multiple retrieval failures seen for Path %q, please retry after %s", e.Path, humanRetry(e.retryAfter))
	case e.Cid != cid.Undef:
		return fmt.Sprintf("multiple retrieval failures seen for CID %q, please retry after %s", e.Cid, humanRetry(e.retryAfter))
	default:
		return fmt.Sprintf("multiple retrieval failures for unknown CID/Path (BUG), please retry after %s", humanRetry(e.retryAfter))
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
type ErrInvalidResponse struct {
	Message string
}

func (e ErrInvalidResponse) Error() string {
	return e.Message
}
