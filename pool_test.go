package caboose

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateWeightWithRefresh(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithWeightChangeDebounce(0))
	ph.StartAndWait(t)

	// downvote first node
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 10)

	// downvote second node again
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 5)

	// upvote node
	ph.upvoteAndAssertUpvoted(t, ph.eps[0], 6)
	ph.upvoteAndAssertUpvoted(t, ph.eps[1], 20)
	ph.upvoteAndAssertUpvoted(t, ph.eps[2], 20)
	ph.downvoteAndAssertDownvoted(t, ph.eps[2], 10)
	ph.upvoteAndAssertUpvoted(t, ph.eps[2], 11)

	// when now is downvoted to zero, it will be added back by a refresh with a weight of 20.
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 3)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 1)
	ph.pool.changeWeight(ph.eps[0], true)

	require.Eventually(t, func() bool {
		ph.pool.lk.RLock()
		defer ph.pool.lk.RUnlock()
		weights := ph.pool.endpoints.ToWeights()

		return weights[ph.eps[0]] == 20 && weights[ph.eps[1]] == 20 && weights[ph.eps[2]] == 11

	}, 10*time.Second, 100*time.Millisecond)
}

func TestUpdateWeightWithMembershipDebounce(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithMembershipDebounce(1000*time.Second), WithWeightChangeDebounce(0))
	ph.StartAndWait(t)

	// assert node is removed when it's weight drops to 0 and not added back
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 10)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 5)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 2)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 1)
	time.Sleep(1 * time.Second)
	ph.downvoteAndAssertRemoved(t, ph.eps[0])
}

func TestUpdateWeightWithoutRefresh(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithWeightChangeDebounce(0))
	ph.StartAndWait(t)
	ph.stopOrch(t)

	// assert node is removed when it's weight drops to 0
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 10)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 5)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 2)
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 1)
	ph.downvoteAndAssertRemoved(t, ph.eps[0])
	ph.assertRingSize(t, 2)
}

func TestUpdateWeightDebounce(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithWeightChangeDebounce(1000*time.Second))
	ph.StartAndWait(t)

	// downvote first node
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 10)

	// downvoting a thousand times does NOT change weight
	for i := 0; i < 1000; i++ {
		ph.downvoteAndAssertDownvoted(t, ph.eps[0], 10)
	}

	// or upvoting
	for i := 0; i < 1000; i++ {
		ph.upvoteAndAssertUpvoted(t, ph.eps[0], 10)
	}
}

func TestUpdateWeightBatched(t *testing.T) {
	ph := BuildPoolHarness(t, 5, WithWeightChangeDebounce(0))
	ph.StartAndWait(t)

	// downvote, 0,2, & 4
	var reqs []batchUpdateReq
	for i := 0; i < 5; i = i + 2 {
		reqs = append(reqs, batchUpdateReq{
			node:     ph.eps[i],
			failure:  true,
			expected: 10,
		})
	}
	ph.updateBatchedAndAssert(t, reqs)

	// upvote, 0,2, & 3
	reqs = []batchUpdateReq{}
	reqs = append(reqs, batchUpdateReq{
		node:     ph.eps[0],
		failure:  false,
		expected: 11,
	}, batchUpdateReq{
		node:     ph.eps[2],
		failure:  false,
		expected: 11,
	}, batchUpdateReq{
		node:     ph.eps[3],
		failure:  false,
		expected: 20,
	})

	ph.updateBatchedAndAssert(t, reqs)

}

type poolHarness struct {
	gol      sync.Mutex
	goodOrch bool
	orchUrl  *url.URL
	pool     *pool
	n        int

	eps []string
}

func (ph *poolHarness) assertRemoved(t *testing.T, url string) {
	ph.pool.lk.RLock()
	defer ph.pool.lk.RUnlock()

	for i := range ph.pool.endpoints {
		if ph.pool.endpoints[i].url == url {
			require.Fail(t, "node not removed")
		}
	}
}

type batchUpdateReq struct {
	node     string
	failure  bool
	expected int
}

func (ph *poolHarness) updateBatchedAndAssert(t *testing.T, reqs []batchUpdateReq) {
	var weightReqs []weightUpdateReq

	for _, req := range reqs {
		weightReqs = append(weightReqs, weightUpdateReq{
			node:    req.node,
			failure: req.failure,
		})
	}

	ph.pool.updateWeightBatched(weightReqs)

	for _, req := range reqs {
		ph.assertWeight(t, req.node, req.expected)
	}
}

func (ph *poolHarness) downvoteAndAssertRemoved(t *testing.T, url string) {
	ph.pool.changeWeight(url, true)
	ph.assertRemoved(t, url)
}

func (ph *poolHarness) downvoteAndAssertDownvoted(t *testing.T, url string, expected int) {
	ph.pool.changeWeight(url, true)
	ph.assertWeight(t, url, expected)
}

func (ph *poolHarness) upvoteAndAssertUpvoted(t *testing.T, url string, expected int) {
	ph.pool.changeWeight(url, false)
	ph.assertWeight(t, url, expected)
}

func (ph *poolHarness) assertWeight(t *testing.T, url string, expected int) {
	ph.pool.lk.RLock()
	defer ph.pool.lk.RUnlock()

	for i := range ph.pool.endpoints {
		if ph.pool.endpoints[i].url == url {
			require.EqualValues(t, expected, ph.pool.endpoints[i].replication)
			return
		}
	}
	require.Fail(t, "not found")
}

func (ph *poolHarness) assertRingSize(t *testing.T, expected int) {
	ph.pool.lk.RLock()
	defer ph.pool.lk.RUnlock()

	require.EqualValues(t, expected, len(ph.pool.endpoints))
}

func (ph *poolHarness) StartAndWait(t *testing.T) {
	ph.pool.Start()
	ph.waitPoolReady(t)
}

func (ph *poolHarness) Start() {
	ph.pool.Start()
}

func (ph *poolHarness) waitPoolReady(t *testing.T) {
	require.Eventually(t, func() bool {
		ph.pool.lk.RLock()
		defer ph.pool.lk.RUnlock()

		return len(ph.pool.endpoints) == ph.n
	}, 10*time.Second, 1*time.Second)
}

func (ph *poolHarness) stopOrch(t *testing.T) {
	ph.gol.Lock()
	defer ph.gol.Unlock()
	ph.goodOrch = false
}

type HarnessOption func(config *Config)

func WithWeightChangeDebounce(debounce time.Duration) func(*Config) {
	return func(config *Config) {
		config.PoolWeightChangeDebounce = debounce
	}
}

func WithPoolRefreshInterval(interval time.Duration) func(*Config) {
	return func(config *Config) {
		config.PoolRefresh = interval
	}
}

func WithMembershipDebounce(debounce time.Duration) func(config *Config) {
	return func(config *Config) {
		config.PoolMembershipDebounce = debounce
	}
}

func BuildPoolHarness(t *testing.T, n int, opts ...HarnessOption) *poolHarness {
	ph := &poolHarness{goodOrch: true, n: n}

	purls := make([]string, n)
	for i := 0; i < n; i++ {
		purls[i] = fmt.Sprintf("someurl%d", i)
	}

	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ph.gol.Lock()
		defer ph.gol.Unlock()
		if ph.goodOrch {
			json.NewEncoder(w).Encode(purls)
		} else {
			json.NewEncoder(w).Encode([]string{})
		}
	}))
	ourl, _ := url.Parse(orch.URL)
	ph.orchUrl = ourl
	config := &Config{
		OrchestratorEndpoint:     ourl,
		OrchestratorClient:       http.DefaultClient,
		PoolWeightChangeDebounce: 100 * time.Millisecond,
		PoolRefresh:              100 * time.Millisecond,
		PoolMembershipDebounce:   100 * time.Millisecond,
		NBackupNodes:             4,
	}

	for _, opt := range opts {
		opt(config)
	}

	var err error
	ph.pool, err = newPool(config)
	require.NoError(t, err)
	ph.eps = purls

	return ph
}
