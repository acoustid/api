package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
)

const BlockSize = 512

func main() {
	fileFlag := flag.String("file", "", "segment file to dump")
	flag.Parse()

	if *fileFlag == "" {
		log.Fatal("no input file")
	}

	file, err := os.Open(*fileFlag)
	if err != nil {
		log.Fatalf("failed to open the file (%v)", err)
	}
	defer file.Close()

	buf := make([]byte, BlockSize)

	i := 0
	for {
		_, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("error while reading block data (%v)", err)
		}

		reader := bytes.NewReader(buf)

		var n uint16
		err = binary.Read(reader, binary.BigEndian, &n)
		if err != nil {
			log.Fatalf("error while parsing block header (%v)", err)
		}

		type KV struct {
			term  uint32
			docID uint32
		}
		block := make([]KV, 0, int(n))

		var term, docID uint32
		for j := 0; j < int(n); j++ {
			var v1 uint64
			if j > 0 {
				v1, err = binary.ReadUvarint(reader)
				if err != nil {
					log.Fatalf("error while parsing block data (%v)", err)
				}
			}
			v2, err := binary.ReadUvarint(reader)
			if err != nil {
				log.Fatalf("error while parsing block data (%v)", err)
			}
			if v1 > 0 {
				docID = 0
			}
			term += uint32(v1)
			docID += uint32(v2)
			//			fmt.Printf("  %v -> %v\n", v1, docID)
			//			fmt.Printf("  %08x -> %v\n", term, docID)
			//			block = append(block, KV{term, docID})
			block = append(block, KV{uint32(v1 >> 4), docID})
		}

		maxDocID := uint32(0)
		minDocID := uint32(math.MaxUint32)
		maxTerm := uint32(0)
		minTerm := uint32(math.MaxUint32)
		for _, kv := range block {
			if kv.docID < minDocID {
				minDocID = kv.docID
			}
			if kv.docID > maxDocID {
				maxDocID = kv.docID
			}
			if kv.term < minTerm {
				minTerm = kv.term
			}
			if kv.term > maxTerm {
				maxTerm = kv.term
			}
		}

		fmt.Printf("#%v items=%v terms=%v termbits=%v docids=%v docidbits=%v\n", i, n,
			maxTerm-minTerm, math.Ceil(math.Log2(float64(maxTerm-minTerm))),
			maxDocID-minDocID, math.Ceil(math.Log2(float64(maxDocID-minDocID))))

		//		for _, kv := range block {
		//			fmt.Printf("  %v -> %v\n", kv.term>>4, kv.docID)
		//			fmt.Printf("  %v -> %v\n", kv.term>>4, kv.docID-minDocID)
		//		}

		i++
	}
}
