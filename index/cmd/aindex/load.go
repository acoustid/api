package main

import (
	"gopkg.in/urfave/cli.v1"
	"github.com/acoustid/go-acoustid/index"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"bufio"
	"strings"
	"strconv"
	"os"
)

func parseCsv(input io.Reader, docs chan index.Doc, quit chan struct{}) error {
	stream := bufio.NewReader(input)
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
		select {
		case docs <- index.Doc{ID: lastDocID, Terms: terms}:
		case <-quit:
			return nil
		}
	}
	return nil
}

var loadCommand = cli.Command{
	Name:  "load",
	Usage: "Load docs into the index",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "dbpath", Usage: "path to the database directory"},
		cli.StringFlag{Name: "fmt, f", Usage: "input format"},
	},
	Action: runLoad,
}

func runLoad(ctx *cli.Context) error {
	path := ctx.String("dbpath")
	if path == "" {
		return errors.New("no database directory specified")
	}

	fs, err := vfs.OpenDir(path, true)
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

	docs := make(chan index.Doc)
	quit := make(chan struct{}, 1)

	defer func() {
		quit <- struct{}{}
	}()

	var parserErr error
	var parser func(input io.Reader, docs chan index.Doc, quit chan struct{}) error

	fmt := ctx.String("fmt")
	switch fmt {
	case "csv":
		parser = parseCsv
	case "":
		return errors.New("input format not specified")
	default:
		return errors.Errorf("unknown format %v", fmt)
	}

	go func() {
		parserErr = parser(os.Stdin, docs, quit)
		close(docs)
	}()

	txn, err := idx.Transaction()
	if err != nil {
		quit <- struct{}{}
		return errors.Wrap(err, "unable to start a transaction")
	}
	defer txn.Close()

	for doc := range docs {
		err = txn.Add(doc.ID, doc.Terms)
		if err != nil {
			return errors.Wrap(err, "add failed")
		}
	}

	if parserErr != nil {
		return errors.Wrap(parserErr, "parser failed")
	}

	err = txn.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}