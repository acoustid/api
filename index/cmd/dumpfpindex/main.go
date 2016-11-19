// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"os"
	"github.com/pkg/errors"
	"github.com/acoustid/go-acoustid/util"
	"fmt"
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

			fmt.Printf("%d %d\n", term>>4, docID)
		}
	}
	return nil
}

func main() {
	var (
		dataFilename = flag.String("d", "", "segment data file to dump")
		indexFilename = flag.String("i", "", "segment index file to dump")
		blockSize = flag.Int("b", 512, "block size")
	)

	flag.Parse()

	if *dataFilename == "" || *indexFilename == "" {
		log.Fatal("no input file")
	}

	blockIndex, err := readBlockIndex(*indexFilename)
	if err != nil {
		log.Fatalf("error while reading index: %v", err)
	}

	err = readData(*dataFilename, *blockSize, blockIndex)
	if err != nil {
		log.Fatalf("error while reading data: %v", err)
	}
}
