package bitset

import (
	"encoding/json"
	"io"
)

type SparseBitSet struct {
	blocks map[uint32][]uint64
}

func NewSparseBitSet() *SparseBitSet {
	return &SparseBitSet{blocks: make(map[uint32][]uint64)}
}

func (s *SparseBitSet) Clone() *SparseBitSet {
	var s2 SparseBitSet
	for i, block := range s.blocks {
		s2.blocks[i] = make([]uint64, len(block))
		copy(s2.blocks[i], block)
	}
	return &s2
}

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

func (s *SparseBitSet) WriteTo(w io.Writer) error {
	// XXX binary encoding
	return json.NewEncoder(w).Encode(s.blocks)
}

func (s *SparseBitSet) ReadFrom(r io.Reader) error {
	// XXX binary encoding
	return json.NewDecoder(r).Decode(&s.blocks)
}