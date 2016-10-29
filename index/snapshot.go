package index

import (
	"github.com/cznic/sortutil"
	"sort"
)

type Snapshot struct {
	db       *DB
	manifest *Manifest
}

func (s *Snapshot) Search(query []uint32) (map[uint32]int, error) {
	sort.Sort(sortutil.Uint32Slice(query))

	type result struct {
		hits map[uint32]int
		err  error
	}
	results := make(chan result)

	segments := s.manifest.Segments
	for _, segment := range segments {
		go func(segment *Segment) {
			hits := make(map[uint32]int)
			err := segment.Search(query, func(docID uint32) { hits[docID] += 1 })
			results <- result{hits: hits, err: err}
		}(segment)
	}

	hits := make(map[uint32]int)
	for i := 0; i < len(segments); i++ {
		res := <-results
		if res.err != nil {
			return nil, res.err
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
