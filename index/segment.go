// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/acoustid/go-acoustid/util"
	"github.com/acoustid/go-acoustid/util/intset"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"io"
	"log"
	"sort"
	"time"
)

const (
	DefaultBlockSize = 1024
	BlockHeaderSize  = 8
	Fixed8BitTerms   = 1 << 15
)

var (
	ErrNoData             = errors.New("no data")
	ErrInvalidBlockHeader = errors.New("invalid block header")
	ErrInvalidBlockData   = errors.New("invalid block data")
	ErrBlockNotFound      = errors.New("block not found")
)

type SegmentMeta struct {
	Checksum       uint32 `json:"checksum"`
	BlockSize      int    `json:"block_size"`
	NumBlocks      int    `json:"blocks"`
	NumDocs        int    `json:"docs"`
	NumDeletedDocs int    `json:"deleted_docs,omitempty"`
	NumItems       int    `json:"items"`
	MinTerm        uint32 `json:"min_term"`
	MaxTerm        uint32 `json:"max_term"`
	MinDocID       uint32 `json:"min_docid"`
	MaxDocID       uint32 `json:"max_docid"`
}

type Segment struct {
	ID          uint32      `json:"id"`
	UpdateID    uint32      `json:"updateid,omitempty"`
	Meta        SegmentMeta `json:"meta"`
	blockIndex  []uint32
	reader      vfs.InputFile
	docs        *intset.SparseBitSet
	deletedDocs *intset.SparseBitSet
	dirty       bool
}

// Size returns the estimated size of the segment file in bytes.  The actual file size might differ.
// This calculation is done based on block statistics.
func (s *Segment) Size() int {
	return s.Meta.NumBlocks * (4 + s.Meta.BlockSize)
}

func (s *Segment) NumDocs() int        { return s.Meta.NumDocs }
func (s *Segment) NumDeletedDocs() int { return s.Meta.NumDeletedDocs }
func (s *Segment) NumLiveDocs() int    { return s.Meta.NumDocs - s.Meta.NumDeletedDocs }
func (s *Segment) NumItems() int       { return s.Meta.NumItems }

func (s *Segment) Clone() *Segment {
	return &Segment{
		ID:          s.ID,
		Meta:        s.Meta,
		blockIndex:  s.blockIndex,
		reader:      s.reader,
		docs:        s.docs,
		deletedDocs: s.deletedDocs,
		dirty:       false,
	}
}

func CreateSegment(fs vfs.FileSystem, id uint32, input ItemReader) (*Segment, error) {
	s := &Segment{
		ID: id,
		Meta: SegmentMeta{
			BlockSize: DefaultBlockSize,
		},
	}

	started := time.Now()

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

	log.Printf("created segment %v (docs=%v, items=%v, blocks=%v, checksum=%v, duration=%s)",
		s.ID, s.Meta.NumDocs, s.Meta.NumItems, s.Meta.NumBlocks, s.Meta.Checksum, time.Since(started))

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

	_, err = file.Seek(int64(s.Meta.BlockSize*s.Meta.NumBlocks), io.SeekStart)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "seek failed")
	}

	blockIndex := make([]uint32, s.Meta.NumBlocks)
	err = binary.Read(file, binary.LittleEndian, blockIndex)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "block index read failed")
	}
	s.blockIndex = blockIndex

	var docs intset.SparseBitSet
	err = docs.Read(file)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "docID set read failed")
	}
	s.docs = &docs

	err = s.LoadUpdate(fs)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "update load failed")
	}

	s.reader = file

	return nil
}

func (s *Segment) fileName() string {
	return fmt.Sprintf("segment-%d.dat", s.ID)
}

func (s *Segment) updateFileName(updateID uint32) string {
	return fmt.Sprintf("segment-%d-%d.del", s.ID, updateID)
}

func (s *Segment) fileNames() []string {
	names := []string{s.fileName()}
	if s.UpdateID != 0 {
		names = append(names, s.updateFileName(s.UpdateID))
	}
	return names
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
	baseTerm := input[0].Term
	lastTerm := baseTerm
	var maxTermDiff uint32
	for _, val := range input {
		if baseDocID > val.DocID {
			baseDocID = val.DocID
		}
		termDiff := val.Term - lastTerm
		if termDiff > maxTermDiff {
			maxTermDiff = termDiff
		}
		lastTerm = val.Term
	}
	lastTerm = baseTerm

	var termBits int
	if maxTermDiff > 0 {
		termBits = 1 + util.HighestSetBit32(maxTermDiff)
	}

	var flags uint16
	if termBits <= 8 {
		flags |= Fixed8BitTerms
		for i, it := range input {
			buf1[ptr1] = byte(it.Term - lastTerm)
			n2 := util.PutUvarint32(buf2[ptr2:], it.DocID-baseDocID)
			if BlockHeaderSize+ptr1+ptr2+1+n2 >= s.Meta.BlockSize {
				n = i
				break
			}
			ptr1++
			ptr2 += n2
			lastTerm = it.Term
			s.Meta.Checksum += it.Term + it.DocID
			s.docs.Add(it.DocID)
		}
	} else {
		for i, it := range input {
			n1 := util.PutUvarint32(buf1[ptr1:], it.Term-lastTerm)
			n2 := util.PutUvarint32(buf2[ptr2:], it.DocID-baseDocID)
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
	}

	if s.Meta.NumBlocks > 0 {
		if s.Meta.MinTerm > baseTerm {
			s.Meta.MinTerm = baseTerm
		}
		if s.Meta.MaxTerm < lastTerm {
			s.Meta.MaxTerm = lastTerm
		}
	} else {
		s.Meta.MinTerm = baseTerm
		s.Meta.MaxTerm = lastTerm
	}

	s.Meta.NumItems += n
	s.Meta.NumBlocks += 1
	s.blockIndex = append(s.blockIndex, baseTerm)

	var header [BlockHeaderSize]byte
	binary.LittleEndian.PutUint16(header[0:], uint16(n)&0x0fff|flags&0xf000)
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

	s.docs = intset.NewSparseBitSet(0)

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

	s.Meta.NumDocs = s.docs.Len()
	s.Meta.MinDocID = s.docs.Min()
	s.Meta.MaxDocID = s.docs.Max()

	err := binary.Write(writer, binary.LittleEndian, s.blockIndex)
	if err != nil {
		return errors.Wrap(err, "block index write failed")
	}

	err = s.docs.Write(writer)
	if err != nil {
		return errors.Wrap(err, "docID set write failed")
	}

	_, err = writer.Write([]byte{0, '\n'})
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
				if s.deletedDocs == nil || !s.deletedDocs.Contains(val.DocID) {
					callback(val.DocID)
				}
			}
		}
		bi++
		if bi == len(blocks) {
			return nil
		}
	}
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

	flags := binary.LittleEndian.Uint16(data)
	n := int(flags & 0x0fff)
	values := make([]Item, n)

	ptr := BlockHeaderSize

	term := s.blockIndex[i]
	if flags&Fixed8BitTerms != 0 {
		for i := range values {
			term += uint32(data[ptr])
			values[i].Term = term
			ptr++
		}
	} else {
		for i := range values {
			delta, nn := util.Uvarint32(data[ptr:])
			if nn <= 0 {
				return nil, ErrInvalidBlockData
			}
			term += delta
			values[i].Term = term
			ptr += nn
		}
	}

	baseDocID := binary.LittleEndian.Uint32(data[4:])
	for i := range values {
		delta, nn := util.Uvarint32(data[ptr:])
		if nn <= 0 {
			return nil, ErrInvalidBlockData
		}
		values[i].DocID = baseDocID + delta
		ptr += nn
	}

	return values, nil
}

// Contains returns true if the segment contains the given docID.
func (s *Segment) Contains(docID uint32) bool {
	if docID < s.Meta.MinDocID || docID > s.Meta.MaxDocID {
		return false
	}
	if !s.docs.Contains(docID) {
		return false
	}
	if s.deletedDocs != nil && s.deletedDocs.Contains(docID) {
		return false
	}
	return true
}

func (s *Segment) Delete(docID uint32) bool {
	if !s.Contains(docID) {
		return false
	}
	if s.deletedDocs == nil {
		s.deletedDocs = intset.NewSparseBitSet(1)
	} else if !s.dirty {
		s.deletedDocs = s.deletedDocs.Clone()
	}
	s.deletedDocs.Add(docID)
	s.dirty = true
	s.Meta.NumDeletedDocs += 1
	return true
}

func (s *Segment) DeleteMulti(docs *intset.SparseBitSet) bool {
	deletedDocs, numDeletedDocs := s.docs.Intersection(docs)
	if numDeletedDocs == 0 {
		return false
	}
	if s.deletedDocs != nil {
		deletedDocs.Union(s.deletedDocs)
		numDeletedDocs = deletedDocs.Len()
		if s.Meta.NumDeletedDocs == numDeletedDocs {
			return false
		}
	}
	s.deletedDocs = deletedDocs
	s.dirty = true
	s.Meta.NumDeletedDocs = numDeletedDocs
	return true
}

func (s *Segment) SaveUpdate(fs vfs.FileSystem, updateID uint32) error {
	if !s.dirty {
		return nil
	}
	err := vfs.WriteFile(fs, s.updateFileName(updateID), s.deletedDocs.Write)
	if err != nil {
		return err
	}
	s.UpdateID = updateID
	s.dirty = false
	return nil
}

func (s *Segment) LoadUpdate(fs vfs.FileSystem) error {
	if s.UpdateID == 0 {
		return nil
	}

	file, err := fs.OpenFile(s.updateFileName(s.UpdateID))
	if err != nil {
		return errors.Wrap(err, "open failed")
	}
	defer file.Close()

	s.deletedDocs = intset.NewSparseBitSet(0)
	err = s.deletedDocs.Read(file)
	if err != nil {
		return errors.Wrap(err, "read failed")
	}

	log.Printf("loaded update %v for segment %v with %v deleted docs", s.UpdateID, s.ID, s.Meta.NumDeletedDocs)
	return nil
}

func (s *Segment) Reader() ItemReader {
	return &segmentReader{Segment: s}
}

type segmentReader struct {
	*Segment
	block int
}

func (r *segmentReader) ReadBlock() ([]Item, error) {
	i := r.block
	if i >= r.Meta.NumBlocks {
		return nil, io.EOF
	}
	r.block++
	items, err := r.Segment.ReadBlock(i)
	if err != nil {
		return nil, err
	}
	if r.Segment.deletedDocs != nil {
		i := 0
		for _, item := range items {
			if !r.Segment.deletedDocs.Contains(item.DocID) {
				items[i] = item
				i++
			}
		}
		items = items[:i]
	}
	return items, nil
}
