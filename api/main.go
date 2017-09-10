// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package api

import (
	"fmt"
	"github.com/acoustid/go-acoustid/api/handlers"
	"gopkg.in/mgo.v2"
	"gopkg.in/urfave/cli.v1"
	"log"
	"net"
	"net/http"
	"strconv"
)

var ApiCommand = cli.Command{
	Name:  "api",
	Usage: "AcoustID API",
	Subcommands: []cli.Command{
		runCommand,
	},
}

var runCommand = cli.Command{
	Name:  "run",
	Usage: "Run the API service",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "host", Value: "localhost", Usage: "address on which to listen"},
		cli.IntFlag{Name: "port", Value: 8080, Usage: "port number on which to listen"},
		cli.StringFlag{Name: "db", Value: "mongodb://localhost/acoustid", Usage: "which database to use"},
	},
	Action: runServer,
}

func runServer(ctx *cli.Context) {
	dbUrl := ctx.String("db")
	session, err := mgo.Dial(dbUrl)
	if err != nil {
		log.Fatalf("Could not connect to the database at %s: %s", dbUrl, err)
	}
	defer session.Close()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Nothing to see here.\n")
	})

	http.Handle("/v2/submit", handlers.NewSubmitHandler(handlers.NewMongoSubmissionStore(session)))
	http.Handle("/v2/lookup", handlers.NewLookupHandler(session))

	addr := net.JoinHostPort(ctx.String("host"), strconv.Itoa(ctx.Int("port")))
	log.Printf("listening on http://%v", addr)

	log.Fatal(http.ListenAndServe(addr, nil))
}
