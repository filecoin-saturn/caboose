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

func TestUpdateWeight(t *testing.T) {
	ph := BuildPoolHarness(t, 3, 0, 1*time.Nanosecond)
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
}

func TestUpdateWeightDebounce(t *testing.T) {
	ph := BuildPoolHarness(t, 3, 1000*time.Second, 1*time.Nanosecond)
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

func TestReplaceNodeToHaveWeight(t *testing.T) {
	ph := BuildPoolHarness(t, 3, 0, 1*time.Second)
	ph.StartAndWait(t)
	ph.stopOrch(t)

	// node is replace
	ph.replaceAndAssert(t, ph.eps[0], 200)
	ph.assertRingSize(t, 3)

	// when weight is 0, node is removed
	ph.removeAndAssertRemoved(t, ph.eps[1])
	ph.assertRingSize(t, 2)

	// when node isn't found, nothing changes.
	nm := NewMember("random", time.Now())
	ph.pool.replaceNodeToHaveWeight(nm)
	ph.assertRingSize(t, 2)

	// when number of endpoints drops below 0, a refresh is triggered.
	ph.removeAndAssertRemoved(t, ph.eps[0])
	ph.assertRingSize(t, 1)
	ph.assertRingSize(t, 1)
	ph.assertRingSize(t, 1)
	ph.assertRingSize(t, 1)

	ph.removeAndAssertRemoved(t, ph.eps[2])
	ph.assertRingSize(t, 0)
	// start the orchestrator so refresh can happen again
	ph.startOrch(t)

	ph.waitPoolReady(t)
}

func TestUpdateWeightBatched(t *testing.T) {
	ph := BuildPoolHarness(t, 5, 0, 1*time.Second)
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

func (ph *poolHarness) replaceAndAssert(t *testing.T, url string, newWeight int) {
	nm := NewMember(url, time.Now())
	nm.replication = newWeight
	ph.pool.replaceNodeToHaveWeight(nm)

	ph.assertWeight(t, url, newWeight)
}

func (ph *poolHarness) removeAndAssertRemoved(t *testing.T, url string) {
	nm := NewMember(url, time.Now())
	nm.replication = 0
	ph.pool.replaceNodeToHaveWeight(nm)

	ph.assertRemoved(t, url)
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
	}, 10*time.Second, 100*time.Millisecond)
}

func (ph *poolHarness) stopOrch(t *testing.T) {
	ph.gol.Lock()
	defer ph.gol.Unlock()
	ph.goodOrch = false
}

func (ph *poolHarness) startOrch(t *testing.T) {
	ph.gol.Lock()
	defer ph.gol.Unlock()
	ph.goodOrch = true
}

func BuildPoolHarness(t *testing.T, n int, debounce time.Duration, poolRefresh time.Duration) *poolHarness {
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
		PoolWeightChangeDebounce: debounce,
		PoolRefresh:              poolRefresh,
	}
	ph.pool = newPool(config)
	ph.eps = purls

	return ph
}
