package main

import (
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/index/server"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"
	"log"
)

var server2Command = cli.Command{
	Name:  "server2",
	Usage: "Runs the index server",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "host, H", Value: "", Usage: "address on which to listen"},
		cli.IntFlag{Name: "port, p", Value: 7765, Usage: "port number on which to listen"},
		cli.StringFlag{Name: "dbpath, d", Usage: "path to the database directory"},
	},
	Action: runServer2,
}

func runServer2(c *cli.Context) error {
	var fs vfs.FileSystem
	path := c.String("dbpath")
	if path == "" {
		fs = vfs.CreateMemDir()
	} else {
		var err error
		fs, err = vfs.OpenDir(path, true)
		if err != nil {
			return errors.Wrap(err, "failed to open the database diretory")
		}
	}
	defer fs.Close()

	log.Printf("opening database in %v", fs)
	idx, err := index.Open(fs, true, nil)
	if err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	defer idx.Close()

	return server.Run(idx, c.String("host"), c.Int("port"))
}
