// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package cmd

import (
	"bufio"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"
	"io"
	"os"
	"strconv"
)

type itemBlockWithErr struct {
	items []index.Item
	err   error
}

type channelReader struct {
	ch <-chan itemBlockWithErr
}

func (r *channelReader) ReadBlock() ([]index.Item, error) {
	block, ok := <-r.ch
	if !ok {
		return nil, io.EOF
	}
	return block.items, block.err
}

func readTextStream(input io.Reader) <-chan itemBlockWithErr {
	ch := make(chan itemBlockWithErr, 1)
	go func() {
		defer close(ch)
		stream := bufio.NewScanner(input)
		stream.Split(bufio.ScanWords)
		for {
			items := make([]index.Item, 1024)
			for i := range items {
				if !stream.Scan() {
					ch <- itemBlockWithErr{items: items[:i]}
					return
				}
				term, err := strconv.ParseUint(stream.Text(), 10, 32)
				if err != nil {
					ch <- itemBlockWithErr{err: errors.Wrap(err, "invalid term")}
					return
				}
				if !stream.Scan() {
					ch <- itemBlockWithErr{err: errors.New("invalid input, missing docID")}
					return
				}
				docID, err := strconv.ParseUint(stream.Text(), 10, 32)
				if err != nil {
					ch <- itemBlockWithErr{err: errors.Wrap(err, "invalid docID")}
					return
				}
				items[i].Term = uint32(term)
				items[i].DocID = uint32(docID)
			}
			ch <- itemBlockWithErr{items: items}
		}
	}()
	return ch
}

var importCommand = cli.Command{
	Name:  "import",
	Usage: "Import a stream of term/docID pairs into the index",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "dbpath", Usage: "path to the database directory"},
	},
	Action: runImport,
}

func runImport(ctx *cli.Context) error {
	fs, err := vfs.OpenDir(ctx.String("dbpath"), true)
	if err != nil {
		return errors.Wrap(err, "unable to open the database directory")
	}

	opts := *index.DefaultOptions
	opts.EnableAutoCompact = false

	idx, err := index.Open(fs, true, &opts)
	if err != nil {
		return errors.Wrap(err, "unable to open the database")
	}
	defer idx.Close()

	reader := channelReader{ch: readTextStream(os.Stdin)}
	return idx.Import(&reader)
}
