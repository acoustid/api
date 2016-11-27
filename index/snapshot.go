// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/pkg/errors"
	"go4.org/sort"
	"go4.org/syncutil"
)

type Snapshot struct {
	manifest *Manifest
	close    syncutil.Once
	closeFn  func(s *Snapshot) error
}

func (s *Snapshot) Search(query []uint32) (map[uint32]int, error) {
	sort.Slice(query, func(i, j int) bool { return query[i] < query[j] })

	segments := s.manifest.Segments

	type result struct {
		hits map[uint32]int
		err  error
	}
	results := make(chan result, len(segments))

	for _, segment := range segments {
		segment := segment
		go func() {
			hits := make(map[uint32]int)
			err := segment.Search(query, func(docID uint32) { hits[docID] += 1 })
			results <- result{hits: hits, err: err}
		}()
	}

	hits := make(map[uint32]int)
	for i := 0; i < len(segments); i++ {
		res := <-results
		if res.err != nil {
			return nil, errors.Wrap(res.err, "segment search failed")
		}
		for docID, count := range res.hits {
			hits[docID] += count
		}
	}
	return hits, nil
}

// Reader creates an ItemReader that iterates over all items in the index.
func (s *Snapshot) Reader() ItemReader {
	var readers []ItemReader
	for _, segment := range s.manifest.Segments {
		readers = append(readers, segment.Reader())
	}
	return MergeItemReaders(readers...)
}

func (s *Snapshot) Close() error {
	return s.close.Do(func() error { return s.closeFn(s) })
}
