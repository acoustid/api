package index

import (
	"github.com/cznic/sortutil"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

type Manifest struct {
	ID       int      `json:"id"`
	Segments []uint32 `json:"segments"`
}

type Index struct {
	dir       Dir
	wlock     sync.Mutex
	rlock     sync.RWMutex
	txid      uint32
	manifest  Manifest
	segments  []*Segment
	BlockSize int
}

func Open(dir Dir) (*Index, error) {
	idx := &Index{
		dir:       dir,
		BlockSize: 4096,
	}
	return idx, nil
}

func (idx *Index) Close() {

}

func (idx *Index) appendSegment(segment *Segment) error {
	idx.wlock.Lock()
	defer idx.wlock.Unlock()

	manifest := Manifest{
		ID:       idx.manifest.ID + 1,
		Segments: append(idx.manifest.Segments, segment.ID()),
	}

	file, err := idx.dir.CreateFile("manifest.json")
	if err != nil {
		return err
	}
	defer file.Close()

	err = file.Commit()
	if err != nil {
		return err
	}

	idx.manifest = manifest

	idx.rlock.Lock()
	idx.segments = append(idx.segments, segment)
	idx.rlock.Unlock()

	return nil
}

func (idx *Index) createSegment(input TermsIterator) (*Segment, error) {
	return CreateSegment(idx.dir, atomic.AddUint32(&idx.txid, 1), input)
}

func (idx *Index) Add(docid uint32, hashes []uint32) error {
	segment, err := idx.createSegment(SingleDocIterator(docid, hashes))
	if err != nil {
		return err
	}

	err = idx.appendSegment(segment)
	if err != nil {
		log.Printf("failed to append new segment to the database (%v)", err)
		segment.Remove()
		return err
	}

	return nil
}

func (idx *Index) DeleteAll() {

}

func (idx *Index) Search(query []uint32) error {
	sort.Sort(sortutil.Uint32Slice(query))

	idx.rlock.RLock()
	segments := idx.segments
	idx.rlock.RUnlock()

	type result struct {
		hits map[uint32]int
		err  error
	}
	results := make([]result, len(segments))

	var wg sync.WaitGroup
	for i, s := range segments {
		wg.Add(1)
		go func(i int, s *Segment) {
			defer wg.Done()
			hits := make(map[uint32]int)
			err := s.Search(query, func(docID uint32) { hits[docID] += 1 })
			results[i] = result{hits: hits, err: err}
		}(i, s)
	}
	wg.Wait()

	hits := map[uint32]int{}
	for _, res := range results {
		if res.err != nil {
			return res.err
		}
		for docID, count := range res.hits {
			hits[docID] += count
		}
	}

	for docID, count := range hits {
		log.Printf("found %v with %v hits", docID, count)
	}
	return nil
}
