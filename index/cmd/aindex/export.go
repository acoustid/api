// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package main

import (
	"bufio"
	"fmt"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"
	"io"
	"os"
)

var exportCommand = cli.Command{
	Name:  "export",
	Usage: "Export all term/docID pairs from the index",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "dbpath", Usage: "path to the database directory"},
	},
	Action: runExport,
}

func runExport(ctx *cli.Context) error {
	fs, err := vfs.OpenDir(ctx.String("dbpath"), true)
	if err != nil {
		return errors.Wrap(err, "unable to open the database directory")
	}

	opts := *index.DefaultOptions
	opts.EnableAutoCompact = false

	idx, err := index.Open(fs, false, &opts)
	if err != nil {
		return errors.Wrap(err, "unable to open the database")
	}
	defer idx.Close()

	snapshot := idx.Snapshot()
	defer snapshot.Close()

	reader := snapshot.Reader()
	writer := bufio.NewWriter(os.Stdout)
	for {
		items, err := reader.ReadBlock()
		for _, item := range items {
			_, err2 := fmt.Fprintf(writer, "%d %d\n", item.Term, item.DocID)
			if err2 != nil {
				return errors.Wrap(err2, "write failed")
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "read failed")
		}

	}
	err = writer.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	return nil
}
