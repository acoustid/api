package index

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"sort"
	"time"
)

const (
	DefaultBlockSize = 1024
)

type segmentMeta struct {
	ID        uint32
	NumDocs   int
	NumBlocks int
	BlockSize int
	Checksum  uint64
	LastTerm  uint32
}

type Segment struct {
	dir        Dir
	meta       segmentMeta
	blockIndex []uint32
	reader     FileReader
}

func CreateSegment(dir Dir, id uint32, input TermsIterator) (*Segment, error) {
	s := &Segment{
		dir: dir,
		meta: segmentMeta{
			ID: id,
			BlockSize: DefaultBlockSize,
		},
	}

	started := time.Now()

	dataFile, err := s.dir.CreateFile(s.dataFileName())
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()

	metaFile, err := s.dir.CreateFile(s.metaFileName())
	if err != nil {
		return nil, err
	}
	defer metaFile.Close()

	err = s.writeDataBlocks(dataFile, input)
	if err != nil {
		return nil, err
	}

	err = s.writeMetadata(metaFile)
	if err != nil {
		return nil, err
	}

	err = dataFile.Commit()
	if err != nil {
		return nil, err
	}

	err = metaFile.Commit()
	if err != nil {
		s.Remove()
		return nil, err
	}

	s.reader, err = s.dir.OpenFile(s.dataFileName())
	if err != nil {
		s.Remove()
		return nil, err
	}

	log.Printf("created new segment %v (docs=%v, blocks=%v, checksum=0x%016x, duration=%s)",
		s.meta.ID, s.meta.NumDocs, s.meta.NumBlocks, s.meta.Checksum, time.Since(started))

	return s, nil
}

func (s *Segment) ID() uint32 { return s.meta.ID }

func (s *Segment) dataFileName() string {
	return fmt.Sprintf("segment-%v.data", s.meta.ID)
}

func (s *Segment) metaFileName() string {
	return fmt.Sprintf("segment-%v.meta.json", s.meta.ID)
}

func (s *Segment) Remove() {
	names := []string{s.dataFileName(), s.metaFileName()}
	for _, name := range names {
		if err := s.dir.RemoveFile(name); err != nil {
			log.Printf("failed to remove segment file %v (%v)", name, err)
		} else {
			log.Printf("removed segment file %v", name)
		}
	}
	return
}

func (s *Segment) writeMetadata(writer io.Writer) error {
	return json.NewEncoder(writer).Encode(s.meta)
}

func (s *Segment) writeDataBlocks(writer io.Writer, it TermsIterator) error {
	input := make([]TermDocID, s.meta.BlockSize/2)

	buf := make([]byte, s.meta.BlockSize)
	ptr := 4

	lastTerm := uint32(0)
	lastDocID := uint32(0)

	for {
		n, err := it.Read(input)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		for _, pair := range input[:n] {
			if ptr+binary.MaxVarintLen32+binary.MaxVarintLen32 >= len(buf) {
				binary.LittleEndian.PutUint32(buf[:4], uint32(ptr))
				for i := ptr; i < len(buf); i++ {
					buf[i] = byte(0)
				}
				_, err := writer.Write(buf)
				if err != nil {
					return err
				}
				ptr = 4
				lastTerm = 0
				lastDocID = 0
			}
			term, docID := pair.Unpack()
			if ptr == 4 {
				s.blockIndex = append(s.blockIndex, term)
			}

			ptr += binary.PutUvarint(buf[ptr:], uint64(term-lastTerm))
			if term == lastTerm {
				ptr += binary.PutUvarint(buf[ptr:], uint64(docID-lastDocID))
			} else {
				ptr += binary.PutUvarint(buf[ptr:], uint64(docID))
			}

			lastTerm = term
			lastDocID = docID
			s.meta.Checksum += pair.Pack()
		}
	}
	if ptr > 4 {
		binary.LittleEndian.PutUint32(buf[:4], uint32(ptr))
		for i := ptr; i < len(buf); i++ {
			buf[i] = byte(0)
		}
		_, err := writer.Write(buf)
		if err != nil {
			return err
		}
	}

	s.meta.NumDocs = it.NumDocs()
	s.meta.NumBlocks = len(s.blockIndex)
	s.meta.LastTerm = lastTerm

	return nil
}

func (s *Segment) Search(query []uint32, callback func(uint32)) error {
	tmp := make([]TermDocID, 256)
	blocks := s.blockIndex
	qi, bi := 0, 0
	MainLoop:
	for qi < len(query) && bi < len(blocks) {
		q := query[qi]
		if s.meta.LastTerm < q {
			break MainLoop
		}
		if blocks[bi] > q {
			qi += sort.Search(len(query)-qi-1, func(i int) bool { return blocks[bi] <= query[qi+i+1] }) + 1
		} else {
			bi += sort.Search(len(blocks)-bi-1, func(i int) bool { return blocks[bi+i+1] >= q })
			matched := true
			for matched {
				reader, err := s.ReadBlock(bi)
				if err != nil {
					return err
				}
				matched = false
				n := 1
				for n > 0 {
					n, err = reader.Read(tmp)
					for _, pair := range tmp[:n] {
						term, docID := pair.Unpack()
						for term > query[qi] {
							qi += 1
							if qi == len(query) {
								break MainLoop
							}
						}
						if term == query[qi] {
							callback(docID)
							matched = true
						} else {
							matched = false
						}
					}
					if err != nil {
						return err
					}
				}
				bi += 1
				if bi == len(blocks) {
					break MainLoop
				}
			}
		}
	}
	return nil
}

func (s *Segment) ReadBlock(i int) (TermsIterator, error) {
	data := make([]byte, s.meta.BlockSize)
	_, err := s.reader.ReadAt(data, int64(i)*int64(s.meta.BlockSize))
	if err != nil {
		return nil, err
	}
	return NewBlockReader(data)
}

var (
	EOF                   = io.EOF
	ErrInvalidBlockHeader = errors.New("invalid block header")
	ErrInvalidBlockData   = errors.New("invalid block data")
)

type blockReader struct {
	data      []byte
	lastTerm  uint32
	lastDocID uint32
}

// MaxBlockSize is the maximum possible size of a block in bytes.
const MaxBlockSize = math.MaxInt32

// NewBlockReader creates a new SegmentReader that iterates over encoded block data.
func NewBlockReader(data []byte) (TermsIterator, error) {
	size := binary.LittleEndian.Uint32(data[:4])
	if size > MaxBlockSize || int(size) <= 4 || int(size) > len(data) {
		return nil, ErrInvalidBlockHeader
	}
	return &blockReader{data: data[4:size]}, nil
}

func (r *blockReader) NumDocs() int {
	panic("NumDocs is not implemented for BlockReader")
}

func (r *blockReader) read(n int, cb func(term uint32, docID uint32) bool) error {
	i := 0
	for len(r.data) > 0 && (n < 0 || i < n) {
		tmpTerm, n1 := binary.Uvarint(r.data)
		if n1 <= 0 || tmpTerm > math.MaxUint32 {
			return ErrInvalidBlockData
		}
		tmpDocID, n2 := binary.Uvarint(r.data[n1:])
		if n2 <= 0 || tmpDocID > math.MaxUint32 {
			return ErrInvalidBlockData
		}
		term := r.lastTerm + uint32(tmpTerm)
		docID := uint32(tmpDocID)
		if tmpTerm == 0 {
			docID += r.lastDocID
		}
		if !cb(term, docID) {
			break
		}
		r.lastTerm = term
		r.lastDocID = docID
		r.data = r.data[n1+n2:]
		i += 1
	}
	return nil
}

func (r *blockReader) SeekTo(query uint32) (found bool, err error) {
	err = r.read(-1, func(term uint32, docID uint32) bool {
		if term >= query {
			if term == query {
				found = true
			}
			return false
		}
		return true
	})
	return
}

func (r *blockReader) Read(result []TermDocID) (n int, err error) {
	err = r.read(len(result), func(term uint32, docID uint32) bool {
		result[n] = PackTermDocID(term, docID)
		n += 1
		return true
	})
	return n, nil
}
