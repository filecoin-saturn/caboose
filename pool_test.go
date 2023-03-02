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
	node1NewWeight := (maxWeight * 80) / 100
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], node1NewWeight)

	// downvote first node again
	node1NewWeight = (node1NewWeight * 80) / 100
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], node1NewWeight)

	// upvote node
	node1NewWeight = node1NewWeight + 1
	ph.upvoteAndAssertUpvoted(t, ph.eps[0], node1NewWeight)

	ph.upvoteAndAssertUpvoted(t, ph.eps[1], 20)
	ph.upvoteAndAssertUpvoted(t, ph.eps[2], 20)
	ph.downvoteAndAssertDownvoted(t, ph.eps[2], 16)
	ph.upvoteAndAssertUpvoted(t, ph.eps[2], 17)

	for {
		node1NewWeight = (node1NewWeight * 80) / 100
		ph.downvoteAndAssertDownvoted(t, ph.eps[0], node1NewWeight)
		if node1NewWeight == 1 {
			break
		}
	}
	ph.pool.changeWeight(ph.eps[0], true)

	// when node is downvoted to zero, it will be added back by a refresh with a weight of 10% max as it has been removed recently.

	require.Eventually(t, func() bool {
		ph.pool.lk.RLock()
		defer ph.pool.lk.RUnlock()
		weights := ph.pool.endpoints.ToWeights()
		return weights[ph.eps[0]] == (maxWeight*10)/100 && weights[ph.eps[1]] == 20 && weights[ph.eps[2]] == 17
	}, 10*time.Second, 100*time.Millisecond)
}

func TestUpdateWeightWithMembershipDebounce(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithMembershipDebounce(1000*time.Second), WithWeightChangeDebounce(0))
	ph.StartAndWait(t)

	// assert node is removed when it's weight drops to 0 and not added back
	node1NewWeight := maxWeight
	for {
		node1NewWeight = (node1NewWeight * 80) / 100
		ph.downvoteAndAssertDownvoted(t, ph.eps[0], node1NewWeight)
		if node1NewWeight == 1 {
			break
		}
	}
	ph.pool.changeWeight(ph.eps[0], true)

	// node is added back but with 10% max weight.
	require.Eventually(t, func() bool {
		ph.pool.lk.RLock()
		defer ph.pool.lk.RUnlock()
		weights := ph.pool.endpoints.ToWeights()
		return weights[ph.eps[0]] == (maxWeight*10)/100
	}, 10*time.Second, 100*time.Millisecond)
}

func TestUpdateWeightWithoutRefresh(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithWeightChangeDebounce(0))
	ph.StartAndWait(t)
	ph.stopOrch(t)

	// assert node is removed when it's weight drops to 0
	node1NewWeight := maxWeight
	for {
		node1NewWeight = (node1NewWeight * 80) / 100
		ph.downvoteAndAssertDownvoted(t, ph.eps[0], node1NewWeight)
		if node1NewWeight == 1 {
			break
		}
	}
	ph.downvoteAndAssertRemoved(t, ph.eps[0])
	ph.assertRingSize(t, 2)
}

func TestUpdateWeightDebounce(t *testing.T) {
	ph := BuildPoolHarness(t, 3, WithWeightChangeDebounce(1000*time.Second))
	ph.StartAndWait(t)

	// downvote first node
	ph.downvoteAndAssertDownvoted(t, ph.eps[0], 16)

	// downvoting a thousand times does NOT change weight
	for i := 0; i < 1000; i++ {
		ph.downvoteAndAssertDownvoted(t, ph.eps[0], 16)
	}

	// or upvoting
	for i := 0; i < 1000; i++ {
		ph.upvoteAndAssertUpvoted(t, ph.eps[0], 16)
	}
}

func TestIsCoolOff(t *testing.T) {
	dur := 50 * time.Millisecond
	ph := BuildPoolHarness(t, 3, WithMaxNCoolOff(2), WithCoolOffDuration(dur))
	ph.StartAndWait(t)

	require.True(t, ph.pool.isCoolOffLocked(ph.eps[0]))
	require.True(t, ph.pool.isCoolOffLocked(ph.eps[0]))
	require.False(t, ph.pool.isCoolOffLocked(ph.eps[0]))

	require.Eventually(t, func() bool {
		ph.pool.lk.RLock()
		_, ok := ph.pool.coolOffCache.Get(ph.eps[0])
		ph.pool.lk.RUnlock()
		return !ok
	}, 10*time.Second, 50*time.Millisecond)
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
			require.EqualValues(t, expected, ph.pool.endpoints[i].weight)
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

func WithCoolOffDuration(dur time.Duration) func(*Config) {
	return func(config *Config) {
		config.SaturnNodeCoolOff = dur
	}
}

func WithMaxNCoolOff(n int) func(*Config) {
	return func(config *Config) {
		config.MaxNCoolOff = n
	}
}

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
	}

	for _, opt := range opts {
		opt(config)
	}

	ph.pool = newPool(config)
	ph.eps = purls

	return ph
}
