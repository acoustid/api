package index

import (
	"github.com/pkg/errors"
)

type Snapshot struct {
	db       *DB
	manifest *Manifest
}

func (s *Snapshot) Search(query []uint32) (map[uint32]int, error) {
	SortUint32s(query)

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

func (s *Snapshot) Close() error {
	return nil
}
