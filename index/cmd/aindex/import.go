package main

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

type block struct {
	items []index.Item
	err   error
}

type channelReader struct {
	ch <-chan block
}

func (r *channelReader) ReadBlock() ([]index.Item, error) {
	block, ok := <-r.ch
	if !ok {
		return nil, io.EOF
	}
	return block.items, block.err
}

func readTextStream(input io.Reader) <-chan block {
	ch := make(chan block, 1)
	go func() {
		defer close(ch)
		stream := bufio.NewScanner(input)
		stream.Split(bufio.ScanWords)
		for {
			items := make([]index.Item, 1024)
			for i := range items {
				if !stream.Scan() {
					ch <- block{items: items[:i]}
					return
				}
				term, err := strconv.ParseUint(stream.Text(), 10, 32)
				if err != nil {
					ch <- block{err: errors.Wrap(err, "invalid term")}
					return
				}
				if !stream.Scan() {
					ch <- block{err: errors.New("invalid input, missing docID")}
					return
				}
				docID, err := strconv.ParseUint(stream.Text(), 10, 32)
				if err != nil {
					ch <- block{err: errors.Wrap(err, "invalid docID")}
					return
				}
				items[i].Term = uint32(term)
				items[i].DocID = uint32(docID)
			}
			ch <- block{items: items}
		}
	}()
	return ch
}

var importCommand = cli.Command{
	Name:  "import",
	Usage: "Import stream of terms into the index",
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

	idx, err := index.Open(fs, true, nil)
	if err != nil {
		return errors.Wrap(err, "unable to open the database")
	}
	defer idx.Close()

	reader := channelReader{ch: readTextStream(os.Stdin)}
	return idx.Import(&reader)
}
