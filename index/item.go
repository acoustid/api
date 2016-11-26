// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/acoustid/go-acoustid/util/intset"
	"io"
	"sort"
)

// Items is one (term,docID) pair in the inverted index.
type Item struct {
	Term  uint32
	DocID uint32
}

type ItemSliceSortedByTerm []Item

func (s ItemSliceSortedByTerm) Len() int { return len(s) }
func (s ItemSliceSortedByTerm) Less(i, j int) bool {
	return s[i].Term < s[j].Term || (s[i].Term == s[j].Term && s[i].DocID < s[j].DocID)
}
func (s ItemSliceSortedByTerm) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// ItemReader is an abstraction for iterating over Items by blocks.
type ItemReader interface {
	// Read reads a block of Items.
	ReadBlock() (items []Item, err error)
}

// ReadAllItems reads all items from reader into a slice.
func ReadAllItems(reader ItemReader) ([]Item, error) {
	var items []Item
	for {
		block, err := reader.ReadBlock()
		items = append(items, block...)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return items, err
		}
	}
}

type ItemBuffer struct {
	numDocs  int
	minDocID uint32
	maxDocID uint32
	items    []Item
	docs     *intset.SparseBitSet
}

func (ib *ItemBuffer) NumDocs() int     { return ib.numDocs }
func (ib *ItemBuffer) NumItems() int    { return len(ib.items) }
func (ib *ItemBuffer) MinDocID() uint32 { return ib.minDocID }
func (ib *ItemBuffer) MaxDocID() uint32 { return ib.maxDocID }
func (ib *ItemBuffer) Empty() bool      { return len(ib.items) == 0 }

func (ib *ItemBuffer) Reset() {
	ib.numDocs = 0
	ib.minDocID = 0
	ib.maxDocID = 0
	ib.items = ib.items[:0]
	ib.docs = intset.NewSparseBitSet(0)
}

func (ib *ItemBuffer) Add(docID uint32, terms []uint32) {
	ib.numDocs += 1
	if ib.numDocs == 1 || ib.minDocID > docID {
		ib.minDocID = docID
	}
	if ib.numDocs == 1 || ib.maxDocID < docID {
		ib.maxDocID = docID
	}
	for _, term := range terms {
		ib.items = append(ib.items, Item{DocID: docID, Term: term})
	}
	if ib.docs == nil {
		ib.docs = intset.NewSparseBitSet(0)
	}
	ib.docs.Add(docID)
}

func (ib *ItemBuffer) Delete(docID uint32) bool {
	if !ib.docs.Contains(docID) {
		return false
	}

	n := 0
	for _, item := range ib.items {
		if item.DocID != docID {
			ib.items[n] = item
			n++
		}
	}

	if n == len(ib.items) {
		return false
	}

	ib.items = ib.items[:n]
	ib.numDocs--

	ib.docs.Remove(docID)
	ib.minDocID = ib.docs.Min()
	ib.maxDocID = ib.docs.Max()

	return true
}

func (ib *ItemBuffer) Reader() ItemReader {
	sort.Sort(ItemSliceSortedByTerm(ib.items))
	return &itemBufferReader{ib: ib}
}

type itemBufferReader struct {
	ib  *ItemBuffer
	pos int
}

func (r *itemBufferReader) ReadBlock() (items []Item, err error) {
	if r.pos >= len(r.ib.items) {
		err = io.EOF
		return
	}
	items = r.ib.items[r.pos:]
	r.pos += len(items)
	return
}

// MergeItemReaders returns an ItemReader that merges the output of multiple source ItemReaders.
func MergeItemReaders(readers ...ItemReader) ItemReader {
	switch len(readers) {
	case 0:
		return nil
	case 1:
		return readers[0]
	case 2:
		if readers[0] == nil {
			return readers[1]
		}
		if readers[1] == nil {
			return readers[0]
		}
		reader := &multiItemReader{reader1: readers[0], reader2: readers[1]}
		reader.init()
		return reader
	}
	mid := len(readers) / 2
	reader1 := MergeItemReaders(readers[:mid]...)
	reader2 := MergeItemReaders(readers[mid:]...)
	return MergeItemReaders(reader1, reader2)
}

type multiItemReader struct {
	reader1, reader2 ItemReader
	block1, block2   []Item
	buf              []Item
}

func (r *multiItemReader) init() {
	r.buf = make([]Item, 1024)
}

func (r *multiItemReader) ReadBlock() (items []Item, err error) {
	if len(r.block1) == 0 && r.reader1 != nil {
		r.block1, err = r.reader1.ReadBlock()
		if err != nil {
			if err != io.EOF {
				return
			}
			r.reader1 = nil
			err = nil
		}
	}

	if len(r.block2) == 0 && r.reader2 != nil {
		r.block2, err = r.reader2.ReadBlock()
		if err != nil {
			if err != io.EOF {
				return
			}
			r.reader2 = nil
			err = nil
		}
	}

	if len(r.block1) > 0 && len(r.block2) > 0 {
		items = r.buf
		for i := range items {
			v1 := r.block1[0]
			v2 := r.block2[0]
			if v1.Term <= v2.Term || (v1.Term == v2.Term && v1.DocID <= v2.DocID) {
				items[i] = v1
				r.block1 = r.block1[1:]
				if len(r.block1) == 0 {
					items = items[:i+1]
					break
				}
			} else {
				items[i] = v2
				r.block2 = r.block2[1:]
				if len(r.block2) == 0 {
					items = items[:i+1]
					break
				}
			}
		}
		return
	}

	if len(r.block1) > 0 {
		items = r.block1
		r.block1 = r.block1[:0]
		return
	}

	if len(r.block2) > 0 {
		items = r.block2
		r.block2 = r.block2[:0]
		return
	}

	err = io.EOF
	return
}
