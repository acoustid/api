// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package intset

import (
	"encoding/binary"
	"github.com/acoustid/go-acoustid/util"
	"go4.org/sort"
	"io"
	"math"
)

const (
	wordBits   = 64
	blockWords = 128 // 1024 bytes
	blockBits  = blockWords * wordBits
)

// SparseBitSet is a set of uint32s implemented as a map of small fixed-width bitsets.
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

func (s *SparseBitSet) Intersection(s2 *SparseBitSet) (*SparseBitSet, int) {
	s3 := NewSparseBitSet(0)
	n := 0
	for i, block2 := range s2.blocks {
		block, exists := s.blocks[i]
		if exists {
			block3 := make([]uint64, blockWords)
			for j := range block2 {
				block3[j] = block[j] & block2[j]
			}
			nn := util.PopCount64Slice(block3)
			if nn != 0 {
				s3.blocks[i] = block3
				n += nn
			}
		}
	}
	return s3, n
}

// Len computes the number of elements in the set. It executes in time proportional to the number of elements.
func (s *SparseBitSet) Len() int {
	var n int
	for _, block := range s.blocks {
		n += util.PopCount64Slice(block)
	}
	return n
}

// Min returns the smallest element in the set.
func (s *SparseBitSet) Min() uint32 {
	for {
		if len(s.blocks) == 0 {
			return 0
		}
		var mi uint32 = math.MaxUint32
		for i := range s.blocks {
			if i < mi {
				mi = i
			}
		}
		block := s.blocks[mi]
		for j := 0; j < blockWords; j++ {
			if block[j] == 0 {
				continue
			}
			for k := 0; k < wordBits; k++ {
				mask := uint64(1) << uint(k)
				if block[j]&mask != 0 {
					return mi*blockBits + uint32(j)*wordBits + uint32(k)
				}
			}
		}
		delete(s.blocks, mi) // found an empty block
	}
}

// Max returns the largest element in the set.
func (s *SparseBitSet) Max() uint32 {
	for {
		if len(s.blocks) == 0 {
			return 0
		}
		var mi uint32 = 0
		for i := range s.blocks {
			if i > mi {
				mi = i
			}
		}
		block := s.blocks[mi]
		for j := blockWords - 1; j >= 0; j-- {
			if block[j] == 0 {
				continue
			}
			for k := wordBits - 1; k >= 0; k-- {
				mask := uint64(1) << uint(k)
				if block[j]&mask != 0 {
					return mi*blockBits + uint32(j)*wordBits + uint32(k)
				}
			}
		}
		delete(s.blocks, mi) // found an empty block
	}
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
	keys := make([]uint32, 0, len(s.blocks))
	for i := range s.blocks {
		keys = append(keys, i)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, i := range keys {
		err = binary.Write(w, binary.LittleEndian, i)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, s.blocks[i])
		if err != nil {
			return err
		}
	}
	return nil
}
