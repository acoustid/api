package cmd

import (
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/index/server"
	"github.com/acoustid/go-acoustid/util/vfs"
	"gopkg.in/urfave/cli.v1"
	"log"
	"net"
	"strconv"
)

var serverCommand = cli.Command{
	Name:  "run",
	Usage: "Run the index service",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "host", Value: "localhost", Usage: "address on which to listen"},
		cli.IntFlag{Name: "port", Value: 7765, Usage: "port number on which to listen"},
		cli.StringFlag{Name: "dbpath", Usage: "path to the database directory (default: keep the index only in memory)"},
	},
	Action: runServer,
}

func runServer(ctx *cli.Context) error {
	var fs vfs.FileSystem
	path := ctx.String("dbpath")
	if path == "" {
		fs = vfs.CreateMemDir()
	} else {
		var err error
		fs, err = vfs.OpenDir(path, true)
		if err != nil {
			log.Fatalf("Failed to open the database directory: %v", err)
		}
	}

	opts := *index.DefaultOptions

	log.Printf("opening database in %v", fs)
	idx, err := index.Open(fs, true, &opts)
	if err != nil {
		log.Fatalf("Failed to open the database: %v", err)
	}
	defer idx.Close()

	addr := net.JoinHostPort(ctx.String("host"), strconv.Itoa(ctx.Int("port")))
	log.Printf("listening on %v", addr)

	return server.ListenAndServe(addr, idx)
}
