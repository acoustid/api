// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

// Package index provides a persistent inverted index structure for searching in 32-bit integers.
//
// The underlying structure is essentially a uint32->uint32 (term->docID) multimap.

package index

import "io"

type Searcher interface {
	io.Closer

	Search(terms []uint32) (map[uint32]int, error)
}

type Writer interface {
	io.Closer

	// Add adds a document to the index. If the document already exists, it is updated.
	Add(docID uint32, terms []uint32) error

	// Delete deletes a document from the index.
	Delete(docID uint32) error
}

type BulkWriter interface {
	Writer

	// Commits applies atomically all previous operations to the index.
	Commit() error
}
