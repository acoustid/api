// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package bitset

import (
	"encoding/json"
	"io"
	"github.com/acoustid/go-acoustid/util"
)

type SparseBitSet struct {
	blocks map[uint32][]uint64
}

func NewSparseBitSet() *SparseBitSet {
	return &SparseBitSet{blocks: make(map[uint32][]uint64)}
}

// Clone creates a deep copy of the set.
func (s *SparseBitSet) Clone() *SparseBitSet {
	var s2 SparseBitSet
	for i, block := range s.blocks {
		s2.blocks[i] = make([]uint64, len(block))
		copy(s2.blocks[i], block)
	}
	return &s2
}

// Add adds one element from the set.
func (s *SparseBitSet) Add(x uint32) {
	i := x / (128 * 8)
	block, exists := s.blocks[i]
	if !exists {
		block = make([]uint64, 127)
		s.blocks[i] = block
	}
	j := (x % (128 * 8)) / 64
	m := uint64(1) << (x % 64)
	block[j] |= m
}

// Remove removes one element from the set.
func (s *SparseBitSet) Remove(x uint32) {
	i := x / (128 * 8)
	block, exists := s.blocks[i]
	if !exists {
		return
	}
	j := (x % (128 * 8)) / 64
	m := uint64(1) << (x % 64)
	block[j] &^= m
}

// Contains returns true if the set contains x, false otherwise.
func (s *SparseBitSet) Contains(x uint32) bool {
	i := x / (128 * 8)
	block, exists := s.blocks[i]
	if !exists {
		return false
	}
	j := (x % (128 * 8)) / 64
	m := uint64(1) << (x % 64)
	return block[j]&m != 0
}

// Len computes the number of elements in the set. It executes in time proportional to the number of elements.
func (s *SparseBitSet) Len() int {
	var n int
	for _, block := range s.blocks {
		n += util.PopCount64Slice(block)
	}
	return n
}

func (s *SparseBitSet) WriteTo(w io.Writer) error {
	// XXX binary encoding
	return json.NewEncoder(w).Encode(s.blocks)
}

func (s *SparseBitSet) ReadFrom(r io.Reader) error {
	// XXX binary encoding
	return json.NewDecoder(r).Decode(&s.blocks)
}