// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package bitset

import (
	"encoding/binary"
	"github.com/acoustid/go-acoustid/util"
	"io"
)

const (
	wordBits   = 64
	blockWords = 32 // 256 bytes
	blockBits  = blockWords * wordBits
)

// SparseBitSet is an efficient set of uint32 elements.
type SparseBitSet struct {
	blocks map[uint32][]uint64
}

// NewSparseBitSet creates a new sparse bitset. The initial capacity can be specified using the size parameter,
// which can be zero if you want the set to dynamically grow.
func NewSparseBitSet(size int) *SparseBitSet {
	var s SparseBitSet
	s.init(size / blockBits)
	return &s
}

func (s *SparseBitSet) init(n int) {
	s.blocks = make(map[uint32][]uint64, n)
}

// Init initializes the set. The initial capacity can be specified using the size parameter,
// which can be zero if you want the set to dynamically grow.
func (s *SparseBitSet) Init(size int) {
	s.init(size / blockBits)
}

// Clone creates a deep copy of the set.
func (s *SparseBitSet) Clone() *SparseBitSet {
	var s2 SparseBitSet
	s2.init(len(s.blocks))
	for i, block := range s.blocks {
		s2.blocks[i] = make([]uint64, len(block))
		copy(s2.blocks[i], block)
	}
	return &s2
}

// Add adds x to the set.
func (s *SparseBitSet) Add(x uint32) {
	i := x / blockBits
	block, exists := s.blocks[i]
	if !exists {
		block = make([]uint64, blockWords)
		s.blocks[i] = block
	}
	j := (x % blockBits) / wordBits
	mask := uint64(1) << (x % wordBits)
	block[j] |= mask
}

// Remove removes x from the set.
func (s *SparseBitSet) Remove(x uint32) {
	i := x / blockBits
	block, exists := s.blocks[i]
	if !exists {
		return
	}
	j := (x % blockBits) / wordBits
	mask := uint64(1) << (x % wordBits)
	block[j] &^= mask
}

// Contains returns true if the set contains x.
func (s *SparseBitSet) Contains(x uint32) bool {
	i := x / blockBits
	block, exists := s.blocks[i]
	if !exists {
		return false
	}
	j := (x % blockBits) / wordBits
	mask := uint64(1) << (x % wordBits)
	return block[j]&mask != 0
}

// Union updates the set to include all elements from s2.
func (s *SparseBitSet) Union(s2 *SparseBitSet) {
	for i, block2 := range s2.blocks {
		block, exists := s.blocks[i]
		if !exists {
			block = make([]uint64, blockWords)
			copy(block, block2)
			s.blocks[i] = block
		} else {
			for j, mask := range block2 {
				block[j] |= mask
			}
		}
	}
}

// Len computes the number of elements in the set. It executes in time proportional to the number of elements.
func (s *SparseBitSet) Len() int {
	var n int
	for _, block := range s.blocks {
		n += util.PopCount64Slice(block)
	}
	return n
}

// Compact removes unused blocks from the set.
func (s *SparseBitSet) Compact() {
	for i, block := range s.blocks {
		n := util.PopCount64Slice(block)
		if n == 0 {
			delete(s.blocks, i)
		}
	}
}

// Read reads the set from r.
func (s *SparseBitSet) Read(r io.Reader) error {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return err
	}
	s.init(int(n))
	for j := 0; j < int(n); j++ {
		var i uint32
		err = binary.Read(r, binary.LittleEndian, &i)
		if err != nil {
			return err
		}
		s.blocks[i] = make([]uint64, blockWords)
		err = binary.Read(r, binary.LittleEndian, s.blocks[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Write writes the set to w.
func (s *SparseBitSet) Write(w io.Writer) error {
	s.Compact()
	err := binary.Write(w, binary.LittleEndian, uint32(len(s.blocks)))
	if err != nil {
		return err
	}
	for i, block := range s.blocks {
		err = binary.Write(w, binary.LittleEndian, i)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, block)
		if err != nil {
			return err
		}
	}
	return nil
}
