package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/filecoin-saturn/caboose"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/urfave/cli/v2"
)

func main() { os.Exit(main1()) }

func main1() int {
	app := &cli.App{
		Name:  "caboose",
		Usage: "shunt some data",
		Action: func(ctx *cli.Context) error {
			args := ctx.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: caboose <cid> <outputfile>")
			}
			root := args.Get(0)
			parseRoot, err := cid.Parse(root)
			if err != nil {
				return err
			}
			out := args.Get(1)

			le, _ := url.Parse("https://twb3qukm2i654i3tnvx36char40aymqq.lambda-url.us-west-2.on.aws/")
			saturnClient := http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						ServerName: "strn.pl",
					},
				},
			}

			cb, err := caboose.NewCaboose(&caboose.Config{
				OrchestratorClient: &http.Client{
					Timeout: 30 * time.Second,
				},

				LoggingEndpoint: *le,
				LoggingClient:   http.DefaultClient,
				LoggingInterval: 5 * time.Second,

				DoValidation: true,
				PoolRefresh:  5 * time.Minute,
				SaturnClient: &saturnClient,
			})
			if err != nil {
				return err
			}

			ls := cidlink.DefaultLinkSystem()
			ls.SetReadStorage(&bsadapter.Adapter{Wrapped: cb})

			if err := carv2.TraverseToFile(context.Background(), &ls, parseRoot, selectorparse.CommonSelector_ExploreAllRecursively, out); err != nil {
				return err
			}
			bs, err := blockstore.OpenReadWrite(out, []cid.Cid{parseRoot})
			if err != nil {
				return err
			}

			return bs.Finalize()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Println(err)
		return 1
	}
	time.Sleep(5 * time.Second)
	return 0
}
