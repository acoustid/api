// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package cmd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/acoustid/go-acoustid/util"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"
	"io"
	"log"
	"os"
)

func readBlockIndex(name string) ([]uint32, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.Wrap(err, "open failed")
	}
	defer file.Close()

	var blockIndex []uint32
	for {
		var x uint32
		err := binary.Read(file, binary.BigEndian, &x)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		blockIndex = append(blockIndex, x)
	}
	return blockIndex, nil
}

func readData(name string, blockSize int, blockIndex []uint32) error {
	file, err := os.Open(name)
	if err != nil {
		return errors.Wrap(err, "open failed")
	}
	defer file.Close()

	buf := make([]byte, blockSize)

	output := bufio.NewWriter(os.Stdout)

	for _, term := range blockIndex {
		_, err := io.ReadFull(file, buf)
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return errors.Wrap(err, "read failed")
		}

		numItems := int(binary.BigEndian.Uint16(buf))
		ptr := 2

		var docID uint32
		for j := 0; j < numItems; j++ {
			if j > 0 {
				delta, n := util.Uvarint32(buf[ptr:])
				if n < 0 {
					return errors.New("error while parsing block data")
				}
				if delta > 0 {
					docID = 0
				}
				term += delta
				ptr += n
			}

			delta, n := util.Uvarint32(buf[ptr:])
			if n < 0 {
				return errors.New("error while parsing block data")
			}
			docID += delta
			ptr += n

			_, err := fmt.Fprintf(output, "%d %d\n", term>>4, docID)
			if err != nil {
				return errors.New("error while writing output")
			}
		}
	}

	err = output.Flush()
	if err != nil {
		return errors.New("error while writing output")
	}

	return nil
}

func dumpOldSegment(ctx *cli.Context) {
	blockIndex, err := readBlockIndex(ctx.String("index"))
	if err != nil {
		log.Fatalf("error while reading index: %v", err)
	}

	err = readData(ctx.String("data"), ctx.Int("block"), blockIndex)
	if err != nil {
		log.Fatalf("error while reading data: %v", err)
	}
}

var dumpOldSegmentCommand = cli.Command{
	Name:  "dumpfpindex",
	Usage: "Dump old acoustid-index segment",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "data, d", Usage: "segment data file"},
		cli.StringFlag{Name: "index, i", Usage: "segment index file"},
		cli.IntFlag{Name: "block, b", Value: 512, Usage: "block size"},
	},
	Action: dumpOldSegment,
	Hidden: true,
}
