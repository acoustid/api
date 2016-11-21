package main

import (
	"bufio"
	"flag"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
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

func parseInput(input io.Reader) <-chan block {
	ch := make(chan block, 1)
	go func() {
		defer close(ch)
		stream := bufio.NewScanner(input)
		for {
			items := make([]index.Item, 1024)
			for i := range items {
				if !stream.Scan() {
					ch <- block{items: items[:i]}
					return
				}
				parts := strings.Split(stream.Text(), " ")
				if len(parts) != 2 {
					ch <- block{err: errors.New("invalid input")}
					return
				}
				term, err := strconv.ParseUint(parts[0], 10, 32)
				if err != nil {
					ch <- block{err: errors.Wrap(err, "invalid input")}
					return
				}
				docID, err := strconv.ParseUint(parts[1], 10, 32)
				if err != nil {
					ch <- block{err: errors.Wrap(err, "invalid input")}
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

func main() {
	var dbPath = flag.String("dbpath", "", "path to the database directory")

	flag.Parse()

	if *dbPath == "" {
		log.Fatal("no --dbpath specified")
	}

	fs, err := vfs.OpenDir(*dbPath, true)
	if err != nil {
		log.Fatalf("Failed to open the database directory: %v", err)
	}
	defer fs.Close()

	idx, err := index.Open(fs, true, nil)
	if err != nil {
		log.Fatalf("Failed to open the index: %v", err)
	}
	defer idx.Close()

	reader := channelReader{ch: parseInput(os.Stdin)}
	err = idx.Import(&reader)
	if err != nil {
		log.Fatalf("Import failed: %v", err)
	}
}
