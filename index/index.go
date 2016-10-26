package index

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"sort"
	"github.com/cznic/sortutil"
)

type IndexState struct {
	TXID     uint32   `json:"txid"`
	Segments []uint32 `json:"segments"`
}

type Index struct {
	dir       Dir
	lock      sync.Mutex
	txid      uint32
	state     IndexState
	segments  []*Segment
	BlockSize int
}

func Open(dir Dir) (*Index, error) {
	idx := &Index{
		dir: dir,
		BlockSize: 4096,
	}
	return idx, nil
}

func (idx *Index) Close() {

}

func (idx *Index) stateFileName() string {
	return filepath.Join(idx.dir.Path(), "state.json")
}

func (idx *Index) newState() IndexState {
	state := IndexState{
		TXID:     atomic.AddUint32(&idx.txid, 1),
		Segments: idx.state.Segments,
	}
	log.Printf("started new transaction %v", state.TXID)
	return state
}

func (idx *Index) addSegment(segment *Segment) error {
	idx.segments = append(idx.segments, segment)
	return nil
}

func (idx *Index) commitState(state IndexState) error {
	name := idx.stateFileName()
	tmpName := fmt.Sprintf("%v.tmp.%v", name, state.TXID)

	file, err := os.Create(tmpName)
	if err != nil {
		log.Printf("failed to create state file %v (%v)", tmpName, err)
		return err
	}
	defer os.Remove(tmpName)

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	log.Printf("state content %s", data)

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	file.Sync()
	file.Close()

	log.Printf("about to commit transaction %v", state.TXID)

	idx.lock.Lock()
	os.Rename(tmpName, name)
	idx.state = state
	idx.lock.Unlock()

	log.Printf("renamed state file %v to %v", tmpName, name)
	log.Printf("commited transaction %v", state.TXID)

	return nil
}

func (idx *Index) Add(id uint32, hashes []uint32) error {
	state := idx.newState()
	segment := NewSegment(idx.dir, state.TXID)

	err := segment.SaveData(SingleDocIterator(id, hashes))
	if err != nil {
		return err
	}

	err = segment.SaveMetadata()
	if err != nil {
		segment.RemoveFiles()
		return err
	}

	err = idx.addSegment(segment)
	if err != nil {
		segment.RemoveFiles()
		return err
	}

	return nil
}

func (idx *Index) DeleteAll() {

}

func (idx *Index) Search(query []uint32) error {
	sort.Sort(sortutil.Uint32Slice(query))

	segments := idx.segments
	n := len(segments)
	log.Printf("searching in %v segments", n)

	results := make([]map[uint32]int, n)

	sem := make(chan error)
	for i, s := range segments {
		go func(i int, s *Segment) {
			results[i] = map[uint32]int{}
			sem <- s.Search(query, func(docID uint32) { results[i][docID] += 1 })
		}(i, s)
	}
	for i := 0; i < n; i++ {
		err := <-sem
		if err != nil {
			return err
		}
	}

	hits := map[uint32]int{}
	for _, partial := range results {
		for docID, count := range partial {
			hits[docID] += count
		}
	}

	for docID, count := range hits {
		log.Printf("found %v with %v hits", docID, count)
	}

	return nil
}
