package caboose

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/filecoin-saturn/caboose/tieredhashing"
	"github.com/golang-jwt/jwt/v5"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
)

type ep struct {
	server   *httptest.Server
	valid    bool
	cnt      int
	httpCode int
	resp     []byte
	lk       sync.Mutex
}

var testBlock = []byte("hello World")

func (e *ep) Setup() {
	e.valid = true
	e.resp = testBlock
	e.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.lk.Lock()
		defer e.lk.Unlock()
		e.cnt++
		if e.valid {
			w.Write(e.resp)
		} else {
			if e.httpCode == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "1")
			}
			if e.httpCode == 0 {
				e.httpCode = 500
			}
			w.WriteHeader(e.httpCode)
			w.Write([]byte("error"))
		}
	}))
}

func TestPoolMiroring(t *testing.T) {
	if unsafe.Sizeof(unsafe.Pointer(nil)) <= 4 {
		t.Skip("skipping for 32bit architectures because too slow")
	}
	opts := []tieredhashing.Option{
		tieredhashing.WithCorrectnessWindowSize(2),
		tieredhashing.WithLatencyWindowSize(2),
		tieredhashing.WithMaxMainTierSize(1),
	}
	ph := BuildPoolHarness(t, 2, opts)

	p := ph.p
	nodes := ph.p.config.OrchestratorOverride
	p.doRefresh()
	p.config.OrchestratorOverride = nil
	p.Start()

	// promote one node to main pool. other will remain in uknown pool.
	eURL := nodes[0].IP
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.RecordSuccess(eURL, tieredhashing.ResponseMetrics{Success: true, TTFBMs: 30, SpeedPerMs: 30})
	p.th.UpdateMainTierWithTopN()

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid

	_, err := p.fetchBlockWith(context.Background(), finalC, "")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	p.Close()

	for _, e := range ph.eps {
		e.lk.Lock()
		defer e.lk.Unlock()
		if e.cnt != 1 {
			t.Fatalf("expected 1 primary fetch, got %d", e.cnt)
		}
	}
}

func TestLoadPool(t *testing.T) {

	t.Run("returns error if JWT generation fails", func(t *testing.T) {

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

		endpoint, _ := url.Parse(server.URL)
		p := &pool{
			config: &Config{
				OrchestratorEndpoint:  endpoint,
				OrchestratorClient:    http.DefaultClient,
				OrchestratorJwtSecret: "", // Empty secret will cause JWT generation to fail
			},
		}

		_, err := p.loadPool()

		assert.Error(t, err)
	})

	t.Run("adds JWT to request if secret is provided", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			assert.NotEmpty(t, authHeader)

			parts := strings.Split(authHeader, " ")
			assert.Equal(t, 2, len(parts))
			assert.Equal(t, "Bearer", parts[0])

			tokenString := parts[1]
			token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return []byte("secret"), nil
			})

			assert.NoError(t, err)
			assert.True(t, token.Valid)
			json.NewEncoder(w).Encode([]tieredhashing.NodeInfo{})
		}))
		defer server.Close()

		endpoint, _ := url.Parse(server.URL)
		p := &pool{
			config: &Config{
				OrchestratorEndpoint:  endpoint,
				OrchestratorClient:    server.Client(),
				OrchestratorJwtSecret: "secret",
			},
		}
		_, err := p.loadPool()
		assert.NoError(t, err)
	})

}

func TestAuthenticateReq(t *testing.T) {

	req, _ := http.NewRequest("GET", "http://example.com", nil)
	testKey := "testKey"

	newReq, err := authenticateReq(req, testKey)

	assert.NoError(t, err, "Error should not occur during authentication")

	assert.NotNil(t, newReq, "Request should be defined")

	authHeader := newReq.Header.Get("Authorization")
	assert.NotEmpty(t, authHeader, "Authorization header should not be empty")

	parts := strings.Split(authHeader, " ")
	assert.Equal(t, 2, len(parts), "Authorization header should have 2 parts")

	tokenPart := parts[1]
	token, err := jwt.Parse(tokenPart, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(testKey), nil
	})
	assert.NoError(t, err, "Error should not occur during parsing JWT")

	claims, _ := token.Claims.(jwt.MapClaims)

	expiresAt, ok := claims["ExpiresAt"].(float64)
	assert.True(t, ok, "ExpiresAt should be a float64")
	assert.True(t, time.Now().Unix() < int64(expiresAt), "Token should not have expired")
}

// TODO: fix this test
// func TestFetchSentinelCid(t *testing.T) {

// 	ph := BuildPoolHarness(t, 10, nil)
// 	ph.p.th.AddOrchestratorNodes(ph.p.config.OrchestratorOverride)
// 	for _, node := range(ph.p.config.OrchestratorOverride) {
// 		err := ph.p.fetchSentinelCid(node.IP)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	}
// }

type PoolHarness struct {
	p   *pool
	eps []*ep
}

func BuildPoolHarness(t *testing.T, n int, opts []tieredhashing.Option) *PoolHarness {

	saturnClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	ls := cidlink.DefaultLinkSystem()
	lsm := memstore.Store{}
	ls.SetReadStorage(&lsm)
	ls.SetWriteStorage(&lsm)
	finalCL := ls.MustStore(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: cid.NewPrefixV1(uint64(multicodec.Raw), uint64(multicodec.Sha2_256))}, basicnode.NewBytes(testBlock))
	finalC := finalCL.(cidlink.Link).Cid
	cw, err := car.NewSelectiveWriter(context.TODO(), &ls, finalC, selectorparse.CommonSelector_MatchAllRecursively)
	if err != nil {
		t.Fatal(err)
	}
	carBytes := bytes.NewBuffer(nil)
	cw.WriteTo(carBytes)

	nodeInfos := make([]tieredhashing.NodeInfo, n)
	eps := make([]*ep, n)

	for i := 0; i < n; i++ {
		eps[i] = &ep{}
		eps[i].Setup()
		eps[i].lk.Lock()
		eps[i].resp = carBytes.Bytes()
		eURL := strings.TrimPrefix(eps[i].server.URL, "https://")
		nodeInfos[i] = tieredhashing.NodeInfo{
			IP:          eURL,
			ID:          eURL,
			Weight:      rand.Intn(100),
			Distance:    rand.Float32(),
			SentinelCid: finalC.String(),
		}
		eps[i].lk.Unlock()

	}

	conf := Config{
		OrchestratorEndpoint: &url.URL{},
		OrchestratorClient:   http.DefaultClient,
		OrchestratorOverride: nodeInfos,
		LoggingEndpoint:      url.URL{},
		LoggingClient:        http.DefaultClient,
		LoggingInterval:      time.Hour,
		SaturnClient:         saturnClient,
		DoValidation:         false,
		PoolRefresh:          time.Minute,
		MaxRetrievalAttempts: 1,
		TieredHashingOpts:    opts,
		MirrorFraction:       1.0,
	}

	ph := &PoolHarness{
		p:   newPool(&conf),
		eps: eps,
	}
	return ph
}
