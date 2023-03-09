package itests

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/ipfs/go-cid"
	golog "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multicodec"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-saturn/caboose"

	blocks2 "github.com/ipfs/go-libipfs/blocks"

	"github.com/stretchr/testify/require"
)

func init() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		panic(err)
	}
	fmt.Println("Current rlimit", rLimit)
	rLimit.Max = 999999
	rLimit.Cur = 999999
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Setting Rlimit ", err)
	}
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)
	}
	fmt.Println("Rlimit Final", rLimit)

	// generate 10,000 blocks.
	for i := 0; i < 10000; i++ {
		blocks = append(blocks, generateBlockWithCid())
	}
	fmt.Println("generated 10,000 blocks")
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func generateBlockWithCid() blocks2.BasicBlock {
	data := randSeq(1024)
	testCid, _ := cid.V1Builder{Codec: uint64(multicodec.Raw), MhType: uint64(multicodec.Sha2_256)}.Sum([]byte(data))
	blk, err := blocks2.NewBlockWithCid([]byte(data), testCid)
	if err != nil {
		panic(err)
	}
	return *blk
}

var (
	maxLatency = 5000
	minLatency = 100
	blocks     []blocks2.BasicBlock
)

func TestPoolHealthFuzz(t *testing.T) {
	t.Skip("Run locally ONLY")
	golog.SetAllLoggers(golog.LevelFatal)
	ctx := context.Background()

	// build 3000 L1s with different failure rates and latencies
	var l1s []*L1Node
	for i := 0; i < 300; i++ {
		latency := time.Duration(rand.Intn(maxLatency-minLatency)+minLatency) * time.Millisecond

		l1 := BuildL1Node(t, 0.7, latency)
		l1s = append(l1s, l1)
	}
	t.Log("created 300 L1s with a success ratio of 70%")

	// build an orchestrator
	orchURL := BuildOrchestrator(t, l1s)
	t.Logf("orchestrator url: %s", orchURL)

	// build caboose blockstore
	cb := BuildCaboose(t, orchURL, 3)
	success := atomic.NewInt32(0)

	// keep checking that the size of the pool never drops below half.
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			select {
			case <-doneCh:
				close(errCh)
				return
			default:
				if len(cb.GetMemberWeights()) != 0 && len(cb.GetMemberWeights()) < (len(l1s)*10)/100 {
					t.Logf("pool size dropped below 10 percent: %d", len(cb.GetMemberWeights()))
					errCh <- errors.New("pool size dropped below 10%")
					close(errCh)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Send 10 parallel requests a 1000 times.
	var wg errgroup.Group
	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			wg.Go(func() error {

				blk := blocks[rand.Intn(len(blocks))]

				b, err := cb.Get(ctx, blk.Cid())
				if err == nil {
					if !bytes.Equal(b.RawData(), blk.RawData()) {
						return errors.New("block data mismatch")
					}
					success.Inc()
				}
				return nil
			})
		}
	}

	require.NoError(t, wg.Wait())
	close(doneCh)
	require.NoError(t, <-errCh, "pool size dropped below 10%")
	t.Logf("successfully finished test, downloaded %d blocks", success.Load())
}

func BuildCaboose(t *testing.T, ourl string, maxRetries int) *caboose.Caboose {
	saturnClient := &http.Client{
		Timeout: 100 * time.Minute,
		Transport: &http.Transport{
			TLSHandshakeTimeout: 10 * time.Minute,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         "example.com",
			},
		},
	}
	ou, _ := url.Parse(ourl)
	conf := &caboose.Config{
		OrchestratorEndpoint:           ou,
		OrchestratorClient:             http.DefaultClient,
		SaturnClient:                   saturnClient,
		DoValidation:                   false,
		PoolWeightChangeDebounce:       time.Duration(100 * time.Millisecond),
		PoolRefresh:                    time.Millisecond * 50,
		MaxRetrievalAttempts:           maxRetries,
		PoolMembershipDebounce:         100 * time.Millisecond,
		LoggingClient:                  http.DefaultClient,
		MaxFetchFailuresBeforeCoolDown: 2,
		FetchKeyCoolDownDuration:       100 * time.Millisecond,
		SaturnNodeCoolOff:              100 * time.Millisecond,
		MaxNCoolOff:                    2,
		NodeSpeedBoostCoolOff:          100 * time.Millisecond,
	}

	bs, err := caboose.NewCaboose(conf)
	require.NoError(t, err)

	return bs
}

func BuildOrchestrator(t *testing.T, l1s []*L1Node) string {
	purls := make([]string, 0, len(l1s))
	for _, l1 := range l1s {
		l1 := l1
		purls = append(purls, strings.TrimPrefix(l1.server.URL, "https://"))
	}
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(purls)

	}))
	return orch.URL
}

func BuildL1Node(t *testing.T, successRatio float64, latency time.Duration) *L1Node {
	l1 := &L1Node{
		successRatio: successRatio,
		latency:      latency,
	}
	l1.Setup(t)
	return l1
}

type L1Node struct {
	latency      time.Duration
	successRatio float64
	server       *httptest.Server
	url          string
}

func (l *L1Node) Setup(t *testing.T) *L1Node {
	l.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x := rand.Float64()
		time.Sleep(l.latency)
		if x <= l.successRatio {
			cid := strings.TrimPrefix(r.URL.Path, "/ipfs/")

			for i := 0; i < len(blocks); i++ {
				if blocks[i].Cid().String() == cid {
					w.Write(blocks[i].RawData())
					return
				}
			}

		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("error"))
		}
	}))
	l.url = l.server.URL
	return l
}
