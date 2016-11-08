package index

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/acoustid/go-acoustid/util/bitset"
	"github.com/acoustid/go-acoustid/util/intcompress"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"log"
	"math"
	"sort"
	"time"
)

const (
	DefaultBlockSize = 1024
	BlockHeaderSize  = 8
)

var (
	ErrNoData             = errors.New("no data")
	ErrInvalidBlockHeader = errors.New("invalid block header")
	ErrInvalidBlockData   = errors.New("invalid block data")
	ErrBlockNotFound      = errors.New("block not found")
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
	Format    int    `json:"format"`
	Checksum  uint32 `json:"checksum"`
	BlockSize int    `json:"blocksize"`
	NumBlocks int    `json:"nblocks"`
	NumDocs   int    `json:"ndocs"`
	NumItems  int    `json:"nitems"`
	MinTerm   uint32 `json:"minterm"`
	MaxTerm   uint32 `json:"maxterm"`
	MinDocID  uint32 `json:"mindocid"`
	MaxDocID  uint32 `json:"maxdocid"`
}

type Segment struct {
	ID         SegmentID   `json:"id"`
	Meta       SegmentMeta `json:"meta"`
	blockIndex []uint32
	docs       *bitset.FixedBitSet
	reader     vfs.InputFile
}

// Size returns the estimated size of the segment file in bytes.  The actual file size might differ.
// This calculation is done based on block statistics.
func (s *Segment) Size() int {
	return s.Meta.NumBlocks * (4 + s.Meta.BlockSize)
}

func CreateSegment(fs vfs.FileSystem, id SegmentID, input ItemReader) (*Segment, error) {
	s := &Segment{
		ID: id,
		Meta: SegmentMeta{
			BlockSize: DefaultBlockSize,
			MinTerm:   math.MaxUint32,
		},
	}

	started := time.Now()

	log.Printf("started segment %v", s.ID)

	name := s.fileName()
	file, err := fs.CreateAtomicFile(name)
	if err != nil {
		return nil, errors.Wrap(err, "create failed")
	}
	defer file.Close()

	err = s.writeData(file, input)
	if err != nil {
		return nil, errors.Wrap(err, "data writing failed")
	}

	err = file.Commit()
	if err != nil {
		return nil, errors.Wrap(err, "file commit failed")
	}

	log.Printf("completed segment %v with data file '%v' (docs=%v, blocks=%v, checksum=0x%08x, duration=%s)",
		s.ID, name, s.Meta.NumDocs, s.Meta.NumBlocks, s.Meta.Checksum, time.Since(started))

	s.reader, err = fs.OpenFile(name)
	if err != nil {
		s.Remove(fs)
		return nil, errors.Wrapf(err, "open failed")
	}

	return s, nil
}

func (s *Segment) Open(fs vfs.FileSystem) error {
	file, err := fs.OpenFile(s.fileName())
	if err != nil {
		return errors.Wrap(err, "open failed")
	}

	_, err = file.Seek(int64(s.Meta.BlockSize*s.Meta.NumBlocks), 0)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "seek failed")
	}

	blockIndex := make([]uint32, s.Meta.NumDocs)
	err = binary.Read(file, binary.LittleEndian, blockIndex)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "block index read failed")
	}

	docs := bitset.New(s.Meta.MinDocID, s.Meta.MinDocID)
	err = docs.Read(file)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "docID set read failed")
	}

	s.blockIndex = blockIndex
	s.docs = docs
	s.reader = file
	return nil
}

func (s *Segment) fileName() string {
	return fmt.Sprintf("segment-%x.dat", uint64(s.ID))
}

// Remove deletes all files associated with the segment
func (s *Segment) Remove(fs vfs.FileSystem) error {
	name := s.fileName()
	if err := fs.Remove(name); err != nil {
		return errors.Wrapf(err, "failed to remove segment file %v", name)
	}
	log.Printf("removed segment file %v", name)
	return nil
}

func (s *Segment) writeBlock(writer *bufio.Writer, input []Item) (n int, err error) {
	n = len(input)
	if n == 0 {
		err = ErrNoData
		return
	}

	buf1 := make([]byte, s.Meta.BlockSize)
	buf2 := make([]byte, s.Meta.BlockSize)
	ptr1, ptr2 := 0, 0

	baseDocID := input[0].DocID
	for _, val := range input {
		if baseDocID > val.DocID {
			baseDocID = val.DocID
		}
	}

	var lastTerm uint32
	for i, it := range input {
		n1 := intcompress.PutUvarint32(buf1[ptr1:], it.Term-lastTerm)
		n2 := intcompress.PutUvarint32(buf2[ptr2:], it.DocID-baseDocID)
		if BlockHeaderSize+ptr1+ptr2+n1+n2 >= s.Meta.BlockSize {
			n = i
			break
		}
		ptr1 += n1
		ptr2 += n2
		lastTerm = it.Term
		s.Meta.Checksum += it.Term + it.DocID
		s.docs.Add(it.DocID)
	}

	s.Meta.NumItems += n
	s.Meta.NumBlocks += 1
	s.blockIndex = append(s.blockIndex, input[0].Term)

	if s.Meta.MinTerm > input[0].Term {
		s.Meta.MinTerm = input[0].Term
	}
	if s.Meta.MaxTerm < input[n-1].Term {
		s.Meta.MaxTerm = input[n-1].Term
	}

	var header [BlockHeaderSize]byte
	binary.LittleEndian.PutUint16(header[0:], uint16(n))
	binary.LittleEndian.PutUint16(header[2:], uint16(ptr1))
	binary.LittleEndian.PutUint32(header[4:], baseDocID)

	_, err = writer.Write(header[:])
	if err != nil {
		return 0, err
	}

	_, err = writer.Write(buf1[:ptr1])
	if err != nil {
		return 0, err
	}

	_, err = writer.Write(buf2[:ptr2])
	if err != nil {
		return 0, err
	}

	for i := BlockHeaderSize + ptr1 + ptr2; i < s.Meta.BlockSize; i++ {
		err = writer.WriteByte(0)
		if err != nil {
			return 0, err
		}
	}

	return n, nil
}

func (s *Segment) writeData(file io.Writer, it ItemReader) error {
	writer := bufio.NewWriter(file)

	s.Meta.NumDocs = it.NumDocs()
	s.Meta.MinDocID = it.MinDocID()
	s.Meta.MaxDocID = it.MaxDocID()

	s.docs = bitset.New(s.Meta.MinDocID, s.Meta.MinDocID)

	maxItemsPerBlock := (s.Meta.BlockSize - BlockHeaderSize) / 2
	remaining := make([]Item, 0, maxItemsPerBlock)
	for {
		block, err := it.ReadBlock()
		if err != nil && err != io.EOF {
			return err
		}
		if len(block) == 0 {
			for len(remaining) > 0 {
				n, err := s.writeBlock(writer, remaining)
				if err != nil {
					return err
				}
				remaining = remaining[n:]
			}
			break
		}
		for len(remaining) > 0 && len(remaining)+len(block) >= maxItemsPerBlock {
			m := len(remaining)
			remaining = append(remaining, block[:maxItemsPerBlock-m:len(block)]...)
			n, err := s.writeBlock(writer, remaining)
			if err != nil {
				return err
			}
			if n >= m {
				block = block[n-m:]
				remaining = remaining[:0]
			} else {
				n := copy(remaining, remaining[n:m])
				remaining = remaining[:n]
			}
		}
		for len(block) >= maxItemsPerBlock {
			n, err := s.writeBlock(writer, block)
			if err != nil {
				return err
			}
			block = block[n:]
		}
		remaining = append(remaining, block...)
	}

	err := binary.Write(writer, binary.LittleEndian, s.blockIndex)
	if err != nil {
		return errors.Wrap(err, "block index write failed")
	}

	err = s.docs.Write(writer)
	if err != nil {
		return errors.Wrap(err, "docID set write failed")
	}

	err = writer.WriteByte(byte(0))
	if err != nil {
		return err
	}

	err = json.NewEncoder(writer).Encode(s.Meta)
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
	if len(query) == 0 && query[0] > s.Meta.MaxTerm {
		return nil
	}
	blocks := s.blockIndex
	qi, bi := 0, 0
	for {
		q := query[qi]
		if blocks[bi] > q {
			qi += sort.Search(len(query)-qi-1, func(i int) bool { return blocks[bi] <= query[qi+i+1] }) + 1
			if qi == len(query) {
				return nil
			}
			q = query[qi]
		}
		bi += sort.Search(len(blocks)-bi-1, func(i int) bool { return blocks[bi+i+1] >= q })
		values, err := s.ReadBlock(bi)
		if err != nil {
			return err
		}
		for _, val := range values {
			for val.Term > q {
				qi++
				if qi == len(query) {
					return nil
				}
				q = query[qi]
			}
			if val.Term == q {
				callback(val.DocID)
			}
		}
		bi++
		if bi == len(blocks) {
			return nil
		}
	}
	return nil
}

func (s *Segment) ReadBlock(i int) ([]Item, error) {
	if i >= s.Meta.NumBlocks {
		return nil, ErrBlockNotFound
	}

	data := make([]byte, s.Meta.BlockSize)
	_, err := s.reader.ReadAt(data, int64(i)*int64(s.Meta.BlockSize))
	if err != nil {
		return nil, err
	}

	if len(data) <= BlockHeaderSize {
		return nil, ErrInvalidBlockHeader
	}

	n := int(binary.LittleEndian.Uint16(data))
	values := make([]Item, n)

	ptr := BlockHeaderSize

	var term uint32
	for i := range values {
		delta, nn := intcompress.Uvarint32(data[ptr:])
		if nn <= 0 {
			return nil, ErrInvalidBlockData
		}
		term += delta
		values[i].Term = term
		ptr += nn
	}

	baseDocID := binary.LittleEndian.Uint32(data[4:])
	for i := range values {
		delta, nn := intcompress.Uvarint32(data[ptr:])
		if nn <= 0 {
			return nil, ErrInvalidBlockData
		}
		values[i].DocID = baseDocID + delta
		ptr += nn
	}

	return values, nil
}
