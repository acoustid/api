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
	"runtime/pprof"
	"unicode/utf8"
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

func splitPsqlCsv(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for width, i := 0, 0; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		switch r {
		case '\t', '\n', ',', '{', '}':
			if i == 0 {
				return width, data[:width], nil
			} else {
				return i, data[:i], nil
			}
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func importPsqlCsv(idx *index.DB, r io.Reader) error {
	tx, err := idx.Transaction()
	if err != nil {
		return errors.Wrap(err, "unable to start transaction")
	}
	defer tx.Close()

	stream := bufio.NewReader(r)
	var lastDocID uint32
	for {
		line, err := stream.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "invalid input")
		}
		columns := strings.Split(line, "\t")
		docID, err := strconv.ParseUint(columns[0], 10, 32)
		if err != nil {
			return errors.Wrapf(err, "invalid input")
		}
		termStrings := strings.Split(strings.Trim(columns[1], "{}\n"), ",")
		terms := make([]uint32, len(termStrings))
		for i, ts := range termStrings {
			term, err := strconv.ParseInt(ts, 10, 32)
			if err != nil {
				return errors.Wrapf(err, "invalid input")
			}
			terms[i] = uint32(term)>>(32-28)
		}
		lastDocID = uint32(docID)
		tx.Add(lastDocID, terms)
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}

func importText(idx *index.DB, r io.Reader) error {
	reader := channelReader{ch: parseInput(r)}
	return idx.Import(&reader)
}

func main() {
	var dbPath = flag.String("dbpath", "", "path to the database directory")
	var psql = flag.Bool("psql", false, "import fingerprints in the postgresql csv format")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	flag.Parse()

	if *dbPath == "" {
		log.Fatal("no --dbpath specified")
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
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

	if *psql {
		err = importPsqlCsv(idx, os.Stdin)
	} else {
		err = importText(idx, os.Stdin)
	}
	if err != nil {
		log.Fatalf("Import failed: %v", err)
	}
}
