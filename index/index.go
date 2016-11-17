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

	// Add inserts a document into the index.
	// Returns a non-nil error if there was a problem and the document was not inserted.
	Add(docID uint32, terms []uint32) error

	// Delete deletes a document from the index.
	Delete(docID uint32) error

	// DeleteAll deletes all documents from the index.
	DeleteAll() error

	// Import inserts a stream of sorted (docID,term) pairs into the index.
	Import(stream ItemReader) error
}

type BulkWriter interface {
	Writer

	// Commits applies atomically all previous operations to the index.
	Commit() error
}
