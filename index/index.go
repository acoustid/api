package index

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cznic/sortutil"
	"github.com/dchest/safefile"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type IndexState struct {
	TXID     uint32   `json:"txid"`
	Segments []uint32 `json:"segments"`
}

type Index struct {
	Path  string
	lock  sync.Mutex
	txid  uint32
	state IndexState

	BlockSize int
}

func Open(path string) (*Index, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		log.Printf("could not determine the absolute path to the index directory (%v)", err)
		return nil, err
	}

	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Creating index directory %s", path)
			err = os.Mkdir(path, 0750)
			if err != nil {
				log.Printf("Could not create the index directory %s", path)
				return nil, err
			}
		} else {
			log.Printf("Could not open the index directory %s", path)
			return nil, err
		}
	} else if !stat.IsDir() {
		log.Printf("Path %s is not a directory", path)
		return nil, errors.New("not a directory")
	}

	idx := &Index{Path: path}

	idx.BlockSize = 4096

	return idx, nil
}

func (idx *Index) Close() {

}

func (idx *Index) segmentFileName(txid uint32) string {
	return path.Join(idx.Path, fmt.Sprintf("segment-%v.dat", txid))
}

func (idx *Index) stateFileName() string {
	return path.Join(idx.Path, "state.json")
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
	//	segments := make([]uint32, len(state.Segments), len(state.Segments) + 1)
	//copy(segments, state.Segments)
	//state.Segments = append(state.Segments, state.TXID)

	//	idx.commitState(state)
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
	segment := NewSegment(state.TXID)

	meta := &segment.meta
	meta.NumDocs = 1

	filename := path.Join(idx.Path, segment.DataFileName())
	file, err := safefile.Create(filename, 0640)
	if err != nil {
		return err
	}
	defer file.Close()

	started := time.Now()
	log.Printf("[Segment-%v] started writing segment data to %v", segment.ID, filename)

	sort.Sort(sortutil.Uint32Slice(hashes))

	buf := make([]byte, idx.BlockSize)
	ptr := 4
	prevHash := uint32(0)
	for _, hash := range hashes {
		if ptr+binary.MaxVarintLen32+binary.MaxVarintLen32 < len(buf) {
			binary.LittleEndian.PutUint32(buf[:4], uint32(ptr))
			file.Write(buf[:ptr])
			meta.Size += ptr
			meta.NumBlocks += 1
			ptr = 4
			prevHash = 0
		}
		if ptr == 4 {
			segment.AddBlock(BlockInfo{FirstHash: hash, Position: segment.meta.Size})
		}
		ptr += binary.PutUvarint(buf[ptr:], uint64(hash-prevHash))
		ptr += binary.PutUvarint(buf[ptr:], uint64(id))
		prevHash = hash
		meta.Checksum += uint64(hash)<<32 | uint64(id)
	}
	if ptr != 0 {
		binary.LittleEndian.PutUint32(buf[:4], uint32(ptr))
		file.Write(buf[:ptr])
		meta.Size += ptr
		meta.NumBlocks += 1
	}

	err = file.Commit()
	if err != nil {
		log.Printf("failed to save segment data (%v)", err)
		return err
	}

	elapsed := time.Since(started)
	log.Printf("[Segment-%v] saved segment data to %v in %s (NumDocs=%v, NumBlocks=%v, Size=%v, Checksum=0x%016X)",
		segment.ID, filename, elapsed, meta.NumDocs, meta.NumBlocks, meta.Size, meta.Checksum)

	err = idx.saveSegmentMeta(segment)
	if err != nil {
		log.Printf("failed to save segment meta (%v)", err)
		return err
	}

	err = idx.addSegment(segment)
	if err != nil {
		segment.RemoveFiles(idx.Path)
		return err
	}

	return nil
}

func (idx *Index) saveSegmentMeta(segment *Segment) error {
	data, err := json.Marshal(segment.meta)
	if err != nil {
		log.Printf("[Segment-%v] error while serializing segment metadata (%v)", segment.ID, err)
		return err
	}

	filename := path.Join(idx.Path, segment.MetaFileName())
	err = safefile.WriteFile(filename, data, 0640)
	if err != nil {
		log.Printf("[Segment-%v] error while saving segment metadata to %v (%v)", segment.ID, filename, err)
		return err
	}

	log.Printf("[Segment-%v] saved segment metadata to %v", segment.ID, filename)
	return nil
}

func (idx *Index) DeleteAll() {

}

func (idx *Index) Search(hashes []uint32) {

}

/*func (index *Index) Begin(write bool) (*Tx, error) {
	tx := &Tx{}
	if (write) {
		tx.init(index, atomic.AddUint64(&index.txid, 1))
	} else {
		tx.init(index, index.txid)
	}
}

func (idx *Index) CommitState() error {
	state := IndexState{ TXID: 23424 }

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s%cSTATE", idx.Path, os.PathSeparator)
	tempFilename := fmt.Sprintf("%s%c.STATE.%d", idx.Path, os.PathSeparator, state.TXID)

	err = ioutil.WriteFile(tempFilename, data, 0640)
	if err != nil {
		return err
	}
	log.Printf("Saved state to %s", tempFilename)

	os.Rename(tempFilename, filename)
	log.Printf("Renamed %s to %s", tempFilename, filename)

	return nil
}

func (idx *Index) Add(id uint32, hashes []uint32) error {
	s := NewSegment()
	s.Add(id, hashes)
	return nil
}

type Segment struct {
	ID uint64
}

//writer := idx.Writer()
//writer.Insert(123, fp)
//writer.Delete(54234)
//writer.Commit()
*/
