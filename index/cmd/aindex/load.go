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

func loadCsv(input io.Reader, batch index.Batch) error {
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
		err = batch.Add(lastDocID, terms)
		if err != nil {
			return errors.Wrap(err, "add failed")
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

	var loader func(input io.Reader, batch index.Batch) error

	fmt := ctx.String("fmt")
	switch fmt {
	case "csv":
		loader = loadCsv
	case "":
		return errors.New("input format not specified")
	default:
		return errors.Errorf("unknown format %v", fmt)
	}

	txn, err := idx.Transaction()
	if err != nil {
		return errors.Wrap(err, "unable to start a transaction")
	}
	defer txn.Close()

	err = loader(os.Stdin, txn)
	if err != nil {
		return errors.Wrap(err, "load failed")
	}

	err = txn.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}
