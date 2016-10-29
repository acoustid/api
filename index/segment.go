package index

import (
	"bufio"
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
	MaxBlockSize     = math.MaxInt32
)

var (
	ErrNoData             = errors.New("no data")
	ErrInvalidBlockHeader = errors.New("invalid block header")
	ErrInvalidBlockData   = errors.New("invalid block data")
)

type SegmentID uint64

func NewSegmentID(txid uint32, counter uint8) SegmentID {
	return SegmentID(uint64(txid)<<8 | uint64(counter))
}

func (id SegmentID) TXID() uint32   { return uint32(id >> 8) }
func (id SegmentID) Counter() uint8 { return uint8(id & 0xff) }

func (id SegmentID) String() string {
	return fmt.Sprintf("%v:%v", id.TXID(), id.Counter())
}

type SegmentMeta struct {
	BlockSize int    `json:"blocksize"`
	NumBlocks int    `json:"nblocks"`
	NumDocs   int    `json:"ndocs"`
	NumTerms  int    `json:"nterms"`
	Checksum  uint32 `json:"checksum"`
	MinDocID  uint32 `json:"mindocid"`
	MaxDocID  uint32 `json:"maxdocid"`
	MinTerm   uint32 `json:"minterm"`
	MaxTerm   uint32 `json:"maxterm"`
}

type Segment struct {
	ID         SegmentID   `json:"id"`
	Meta       SegmentMeta `json:"meta"`
	dir        Dir
	blockIndex []uint32
	reader     FileReader
}

func CreateSegment(dir Dir, id SegmentID, input TermsIterator) (*Segment, error) {
	s := &Segment{
		ID: id,
		Meta: SegmentMeta{
			BlockSize: DefaultBlockSize,
			MinDocID:  math.MaxUint32,
			MinTerm:   math.MaxUint32,
		},
		dir: dir,
	}

	started := time.Now()

	log.Printf("started segment %v", s.ID)

	name := s.fileName()
	file, err := s.dir.CreateFile(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	err = s.writeData(file, input)
	if err != nil {
		return nil, err
	}

	err = file.Commit()
	if err != nil {
		return nil, err
	}

	log.Printf("completed segment %v with data file '%v' (docs=%v, blocks=%v, checksum=0x%08x, duration=%s)",
		s.ID, name, s.Meta.NumDocs, s.Meta.NumBlocks, s.Meta.Checksum, time.Since(started))

	s.reader, err = s.dir.OpenFile(name)
	if err != nil {
		s.Remove()
		return nil, err
	}

	return s, nil
}

func (s *Segment) Open(dir Dir) error {
	file, err := dir.OpenFile(s.fileName())
	if err != nil {
		return err
	}
	blockIndex := make([]uint32, s.Meta.NumDocs)
	_, err = file.Seek(int64(s.Meta.BlockSize*s.Meta.NumBlocks), 0)
	if err != nil {
		return err
	}
	err = binary.Read(file, binary.LittleEndian, blockIndex)
	if err != nil {
		return err
	}
	s.dir = dir
	s.reader = file
	s.blockIndex = blockIndex
	return nil
}

func (s *Segment) fileName() string {
	return fmt.Sprintf("segment-%x.dat", uint64(s.ID))
}

// Remove deletes all files associated with the segment
func (s *Segment) Remove() error {
	name := s.fileName()
	if err := s.dir.RemoveFile(name); err != nil {
		log.Printf("failed to remove segment file %v (%v)", name, err)
		return err
	}
	log.Printf("removed segment file %v", name)
	return nil
}

func (s *Segment) writeData(file io.Writer, it TermsIterator) error {
	writer := bufio.NewWriter(file)

	input := make([]TermDocID, s.Meta.BlockSize)

	buf := make([]byte, s.Meta.BlockSize)
	ptr := 4

	lastTerm := uint32(0)
	lastDocID := uint32(0)

	s.Meta.NumDocs = it.NumDocs()

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
				s.Meta.NumBlocks += 1
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
			s.Meta.NumTerms += 1
			s.Meta.Checksum += term + docID
			if s.Meta.MinDocID > docID {
				s.Meta.MinDocID = docID
			}
			if s.Meta.MaxDocID < docID {
				s.Meta.MaxDocID = docID
			}
			if s.Meta.MinTerm > term {
				s.Meta.MinTerm = term
			}
			if s.Meta.MaxTerm < term {
				s.Meta.MaxTerm = term
			}
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

	if len(s.blockIndex) == 0 {
		return ErrNoData
	}

	binary.Write(writer, binary.LittleEndian, s.blockIndex)

	writer.WriteByte(byte(0))
	err := json.NewEncoder(writer).Encode(s.Meta)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (s *Segment) Search(query []uint32, callback func(uint32)) error {
	tmp := make([]TermDocID, 256)
	blocks := s.blockIndex
	qi, bi := 0, 0
MainLoop:
	for qi < len(query) && bi < len(blocks) {
		q := query[qi]
		if s.Meta.MaxTerm < q {
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
	data := make([]byte, s.Meta.BlockSize)
	_, err := s.reader.ReadAt(data, int64(i)*int64(s.Meta.BlockSize))
	if err != nil {
		return nil, err
	}
	return NewBlockReader(data)
}

type blockReader struct {
	data      []byte
	lastTerm  uint32
	lastDocID uint32
}

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
